(ns metabase.test.data.datasets
  "Interface + implementations for loading test datasets for different drivers, and getting information about the dataset's tables, fields, etc."
  (:require [clojure.string :as s]
            [clojure.tools.logging :as log]
            [colorize.core :as color]
            [environ.core :refer [env]]
            [expectations :refer :all]
            [metabase.db :refer :all]
            [metabase.driver :as driver]
            (metabase.models [field :refer [Field]]
                             [table :refer [Table]])
            (metabase.test.data [dataset-definitions :as defs]
                                [h2 :as h2]
                                [mongo :as mongo]
                                [mysql :as mysql]
                                [postgres :as postgres]
                                [sqlite :as sqlite]
                                [sqlserver :as sqlserver])
            [metabase.util :as u]))

;; # IDataset

(defprotocol IDataset
  "Functions needed to fetch test data for various drivers."
  (load-data! [this]
    "Load the test data for this dataset.")
  (dataset-loader [this]
    "Return a dataset loader (an object that implements `IDatasetLoader`) for this dataset/driver.")
  (db [this]
    "Return `Database` containing test data for this driver.")
  (default-schema [this]
    "Return the default schema name that tables for this DB should be expected to have.")
  (format-name [this table-or-field-name]
    "Transform a lowercase string `Table` or `Field` name in a way appropriate for this dataset
     (e.g., `h2` would want to upcase these names; `mongo` would want to use `\"_id\"` in place of `\"id\"`.")
  (id-field-type [this]
    "Return the `base_type` of the `id` `Field` (e.g. `:IntegerField` or `:BigIntegerField`).")
  (sum-field-type [this]
    "Return the `base_type` of a aggregate summed field.")
  (timestamp-field-type [this]
    "Return the `base_type` of a `TIMESTAMP` `Field` like `users.last_login`."))


;; # Implementations

(defn- generic-load-data! [{:keys [dbpromise], :as this}]
  (when-not (realized? dbpromise)
    (deliver dbpromise (@(resolve 'metabase.test.data/get-or-create-database!) (dataset-loader this) defs/test-data)))
  @dbpromise)

;; ## Mongo

(deftype MongoDriverData [dbpromise])

(extend MongoDriverData
  IDataset
  {:load-data!           generic-load-data!
   :db                   generic-load-data!
   :dataset-loader       (fn [_] (mongo/dataset-loader))
   :format-name          (fn [_ table-or-field-name]
                           (if (= table-or-field-name "id") "_id"
                               table-or-field-name))
   :default-schema       (constantly nil)
   :id-field-type        (constantly :IntegerField)
   :sum-field-type       (constantly :IntegerField)
   :timestamp-field-type (constantly :DateField)})


;; ## Generic SQL

(def ^:private GenericSQLIDatasetMixin
  {:load-data!           generic-load-data!
   :db                   generic-load-data!
   :format-name          (fn [_ table-or-field-name]
                           table-or-field-name)
   :default-schema       (constantly nil)
   :timestamp-field-type (constantly :DateTimeField)
   :id-field-type        (constantly :IntegerField)
   :sum-field-type       (constantly :BigIntegerField)})


;;; ### H2

(defrecord H2DriverData [dbpromise])

(extend H2DriverData
  IDataset
  (merge GenericSQLIDatasetMixin
         {:dataset-loader    (fn [_]
                               (h2/->H2DatasetLoader))
          :default-schema    (constantly "PUBLIC")
          :format-name       (fn [_ table-or-field-name]
                               (clojure.string/upper-case table-or-field-name))
          :id-field-type     (constantly :BigIntegerField)}))


;;; ### Postgres

(defrecord PostgresDriverData [dbpromise])

(extend PostgresDriverData
  IDataset
  (merge GenericSQLIDatasetMixin
         {:dataset-loader (fn [_]
                            (postgres/->PostgresDatasetLoader))
          :default-schema (constantly "public")
          :sum-field-type (constantly :IntegerField)}))


;;; ### MySQL

(defrecord MySQLDriverData [dbpromise])

(extend MySQLDriverData
  IDataset
  (merge GenericSQLIDatasetMixin
         {:dataset-loader (fn [_]
                            (mysql/->MySQLDatasetLoader))}))

;;; ### SQLite

(defrecord SQLiteDriverData [dbpromise])

(extend SQLiteDriverData
  IDataset
  (merge GenericSQLIDatasetMixin
         {:dataset-loader (fn [_]
                            (sqlite/->SQLiteDatasetLoader))}))


;;; ### SQLServer

(defrecord SQLServerDriverData [dbpromise])

(extend SQLServerDriverData
  IDataset
  (merge GenericSQLIDatasetMixin
         {:dataset-loader (fn [_]
                            (sqlserver/->SQLServerDatasetLoader))
          :default-schema (constantly "dbo")
          :sum-field-type (constantly :IntegerField)}))


;; # Concrete Instances

(def ^:private dataset-name->dataset*
  "Map of dataset keyword name -> dataset instance (i.e., an object that implements `IDataset`)."
  {:mongo     (MongoDriverData.     (promise))
   :h2        (H2DriverData.        (promise))
   :postgres  (PostgresDriverData.  (promise))
   :mysql     (MySQLDriverData.     (promise))
   :sqlite    (SQLiteDriverData.    (promise))
   :sqlserver (SQLServerDriverData. (promise))})

(def ^:const all-valid-dataset-names
  "Set of names of all valid datasets."
  (set (keys dataset-name->dataset*)))

(defn dataset-name->dataset [engine]
  (or (dataset-name->dataset* engine)
      (throw (Exception.(format "Invalid engine: %s\nMust be one of: %s" engine all-valid-dataset-names)))))


;; # Logic for determining which datasets to test against

;; By default, we'll test against against only the :h2 (H2) dataset; otherwise, you can specify which
;; datasets to test against by setting the env var `MB_TEST_DATASETS` to a comma-separated list of dataset names, e.g.
;;
;;    # test against :h2 and :mongo
;;    MB_TEST_DATASETS=generic-sql,mongo
;;
;;    # just test against :h2 (default)
;;    MB_TEST_DATASETS=generic-sql

(defn- get-test-datasets-from-env
  "Return a set of dataset names to test against from the env var `MB_TEST_DATASETS`."
  []
  (when-let [env-drivers (some-> (env :mb-test-datasets)
                                 s/lower-case)]
    (some->> (s/split env-drivers #",")
             (map keyword)
             ;; Double check that the specified datasets are all valid
             (map (fn [dataset-name]
                    (assert (contains? all-valid-dataset-names dataset-name)
                      (format "Invalid dataset specified in MB_TEST_DATASETS: %s" (name dataset-name)))
                    dataset-name))
             set)))

(defonce ^:const
  ^{:doc (str "Set of names of drivers we should run tests against. "
              "By default, this only contains `:h2` but can be overriden by setting env var `MB_TEST_DATASETS`.")}
  test-dataset-names
  (let [datasets (or (get-test-datasets-from-env)
                     #{:h2})]
    (log/info (color/green "Running QP tests against these datasets: " datasets))
    datasets))


;; # Helper Macros

(def ^:dynamic *dataset*
  "The dataset we're currently testing against, bound by `with-dataset`.
   Defaults to `(dataset-name->dataset :h2)`."
  (dataset-name->dataset (if (contains? test-dataset-names :h2) :h2
                             (first test-dataset-names))))

(def ^:dynamic *engine*
  "Keyword name of the engine that we're currently testing against. Defaults to `:h2`."
  :h2)

(defmacro with-dataset
  "Bind `*dataset*` to the dataset with DATASET-NAME and execute BODY."
  [dataset-name & body]
  `(let [engine# ~dataset-name]
     (binding [*engine*  engine#
               *dataset* (dataset-name->dataset engine#)]
       ~@body)))

(defmacro when-testing-dataset
  "Execute BODY only if we're currently testing against DATASET-NAME."
  [dataset-name & body]
  `(when (contains? test-dataset-names ~dataset-name)
     ~@body))

(defmacro with-dataset-when-testing
  "When testing DATASET-NAME, binding `*dataset*` and executes BODY."
  [dataset-name & body]
  `(when-testing-dataset ~dataset-name
     (with-dataset ~dataset-name
       ~@body)))

(defmacro expect-when-testing-dataset
  "Generate a unit test that only runs if we're currently testing against DATASET-NAME."
  [dataset-name expected actual]
  `(expect
       (when-testing-dataset ~dataset-name
         ~expected)
     (when-testing-dataset ~dataset-name
       ~actual)))

(defmacro expect-with-dataset
  "Generate a unit test that only runs if we're currently testing against DATASET-NAME, and that binds `*dataset*` to the current dataset."
  [dataset-name expected actual]
  `(expect-when-testing-dataset ~dataset-name
     (with-dataset ~dataset-name
       ~expected)
     (with-dataset ~dataset-name
       ~actual)))

(defmacro expect-with-datasets
  "Generate unit tests for all datasets in DATASET-NAMES; each test will only run if we're currently testing the corresponding dataset.
   `*dataset*` is bound to the current dataset inside each test."
  [dataset-names expected actual]
  `(do ~@(for [dataset-name (eval dataset-names)]
           `(expect-with-dataset ~dataset-name ~expected ~actual))))

(defmacro expect-with-all-datasets
  "Generate unit tests for all valid datasets; each test will only run if we're currently testing the corresponding dataset.
  `*dataset*` is bound to the current dataset inside each test."
  [expected actual]
  `(expect-with-datasets ~all-valid-dataset-names ~expected ~actual))

(defmacro dataset-case
  "Case statement that switches off of the current dataset.

     (dataset-case
       :h2       ...
       :postgres ...)"
  [& pairs]
  `(cond ~@(mapcat (fn [[dataset then]]
                     (assert (contains? all-valid-dataset-names dataset))
                     [`(= *engine* ~dataset)
                      then])
                   (partition 2 pairs))))
