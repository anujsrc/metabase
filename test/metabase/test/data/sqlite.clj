(ns metabase.test.data.sqlite
  (:require [clojure.string :as s]
            (metabase.test.data [generic-sql :as generic]
                                [interface :as i])))

(defn- database->connection-details
  [_ context {:keys [short-lived?], :as dbdef}]
  {:short-lived? short-lived?
   :db           (str (i/escaped-name dbdef) ".sqlite")})

(def ^:private ^:const field-base-type->sql-type
  {:BigIntegerField "BIGINT"
   :BooleanField    "BOOLEAN"
   :CharField       "VARCHAR(254)"
   :DateField       "DATE"
   :DateTimeField   "DATETIME"
   :DecimalField    "DECIMAL"
   :FloatField      "DOUBLE"
   :IntegerField    "INTEGER"
   :TextField       "TEXT"
   :TimeField       "TIME"})

(defrecord SQLiteDatasetLoader [])

(extend SQLiteDatasetLoader
  generic/IGenericSQLDatasetLoader
  (merge generic/DefaultsMixin
         {:add-fk-sql                (constantly nil) ; TODO - fix me
          :create-db-sql             (constantly nil)
          :drop-db-if-exists-sql     (constantly nil)
          :execute-sql!              (fn [this context dbdef sql]
                                       (when-not (s/blank? sql)
                                         (println (format "SQL [%s] '%s'" context sql))
                                         (generic/sequentially-execute-sql! this context dbdef sql)))
          :pk-sql-type               (constantly "INTEGER")
          :field-base-type->sql-type (fn [_ base-type]
                                       (field-base-type->sql-type base-type))})
  i/IDatasetLoader
  (merge generic/IDatasetLoaderMixin
         {:database->connection-details database->connection-details
          :engine                       (constantly :sqlite)}))
