(ns metabase.driver.sqlite
  (:require (clojure [set :as set]
                     [string :as s])
            (korma [core :as k]
                   [db :as kdb])
            [korma.sql.utils :as kutils]
            (metabase [config :as config]
                      [driver :refer [defdriver]])
            [metabase.driver.generic-sql :refer [sql-driver]]
            [metabase.util :as u]))

;; We'll do regex pattern matching here for determining Field types
;; because SQLite types can have optional lengths, e.g. NVARCHAR(100) or NUMERIC(10,5)
;; See also http://www.sqlite.org/datatype3.html
(def ^:private ^:const pattern->type
  [[#"BIGINT"   :BigIntegerField]
   [#"BIG INT"  :BigIntegerField]
   [#"INT"      :IntegerField]
   [#"CHAR"     :TextField]
   [#"TEXT"     :TextField]
   [#"CLOB"     :TextField]
   [#"BLOB"     :UnknownField]
   [#"REAL"     :FloatField]
   [#"DOUB"     :FloatField]
   [#"FLOA"     :FloatField]
   [#"NUMERIC"  :FloatField]
   [#"DECIMAL"  :DecimalField]
   [#"BOOLEAN"  :BooleanField]
   [#"DATETIME" :DateTimeField]
   [#"DATE"     :DateField]])

(defn- column->base-type [column-type]
  (let [column-type (name column-type)]
    (loop [[[pattern base-type] & more] pattern->type]
      (cond
        (re-find pattern column-type) base-type
        (seq more)                    (recur more)))))

(defn- date
  "Apply truncation / extraction to a date field or value for SQLite.
   See also the [SQLite Date and Time Functions Reference](http://www.sqlite.org/lang_datefunc.html)."
  [unit field-or-value]
  (let [date           (comp (partial kutils/func "DATE(%s)") vector)
        literal        #(k/raw (str \' % \'))
        ;; Convert Timestamps to ISO 8601 strings before passing to SQLite, otherwise they don't seem to work correctly
        v              (if (instance? java.sql.Timestamp field-or-value)
                         (literal (u/date->iso-8601 field-or-value))
                         field-or-value)
        strftime       (fn [format-str] (kutils/func (format "STRFTIME('%s', %%s)" (s/replace format-str "%" "%%"))
                                                     [v]))]
    (case unit
      :default         (kutils/func "DATETIME(%s)" [v])
      :minute          (date (strftime "%Y-%m-%d %H:%M"))
      :minute-of-hour  (strftime "%M")
      :hour            (date (strftime "%Y-%m-%d %H:00"))
      :hour-of-day     (strftime "%H")
      :day             (date v)
      ;; SQLite day of week (%w) is Sunday = 0 <-> Saturday = 6. We want 1 - 7 so add 1
      :day-of-week     (kutils/func "(%s + 1)" [(strftime "%w")])
      :day-of-month    (strftime "%d")
      :day-of-year     (strftime "%j")
      ;; Move back 6 days, then forward to the next Sunday
      :week            (date v, (literal "-6 days"), (literal "weekday 0"))
      ;; SQLite first week of year is 0, so add 1
      :week-of-year    (kutils/func "(%s + 1)" [(strftime "%W")])
      :month           (date v, (literal "start of month"))
      :month-of-year   (strftime "%m")
      ;;    DATE(DATE(%s, 'start of month'), '-' || ((STRFTIME('%m', %s) - 1) % 3) || ' months')
      ;; -> DATE(DATE('2015-11-16', 'start of month'), '-' || ((STRFTIME('%m', '2015-11-16') - 1) % 3) || ' months')
      ;; -> DATE('2015-11-01', '-' || ((11 - 1) % 3) || ' months')
      ;; -> DATE('2015-11-01', '-' || 1 || ' months')
      ;; -> DATE('2015-11-01', '-1 months')
      ;; -> '2015-10-01'
      :quarter         (date
                        (date v, (literal "start of month"))
                        (kutils/func "'-' || ((%s - 1) %% 3) || ' months'"
                                     [(strftime "%m")]))
      ;; q = (m + 2) / 3
      :quarter-of-year (kutils/func "((%s + 2) / 3)"
                                    [(strftime "%m")])
      :year            (strftime "%Y"))))

(defn- date-interval [unit amount]
  (let [[multiplier unit] (case unit
                            :second  [1 "seconds"]
                            :minute  [1 "minutes"]
                            :hour    [1 "hours"]
                            :day     [1 "days"]
                            :week    [7 "days"]
                            :month   [1 "months"]
                            :quarter [3 "months"]
                            :year    [1 "years"])]
    ;; Make a string like DATE('now', '+7 days')
    (k/raw (format "DATE('now', '%+d %s')" (* amount multiplier) unit))))

(defn- unix-timestamp->timestamp [field-or-value seconds-or-milliseconds]
  (kutils/func (case seconds-or-milliseconds
                 :seconds      "DATETIME(%s, 'unixepoch')"
                 :milliseconds "DATETIME(%s / 1000, 'unixepoch')")
               [field-or-value]))

(defdriver sqlite
  (cond-> (sql-driver {:driver-name               "SQLite"
                       :details-fields            [{:name         "db"
                                                    :display-name "Filename"
                                                    :placeholder  "/home/camsaul/toucan_sightings.sqlite 😋"
                                                    :required     true}]
                       :column->base-type         column->base-type
                       :string-length-fn          :LENGTH
                       :current-datetime-fn       (k/raw "DATE('now')")
                       :connection-details->spec  kdb/sqlite3
                       :date                      date
                       :date-interval             date-interval
                       :unix-timestamp->timestamp unix-timestamp->timestamp})
    ;; HACK SQLite doesn't support ALTER TABLE ADD CONSTRAINT FOREIGN KEY and I don't have all day to work around this
    ;; so for now we'll just skip the foreign key stuff in the tests.
    (config/is-test?) (update :features set/difference #{:foreign-keys}))
  )
