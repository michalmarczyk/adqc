(ns adqc.lqp-test
  (:use clojure.test)
  (:use [adqc sql lqp]
        [adqc.lqp operators])
  (:import [uk.org.ogsadai.dqp.common DataNode]
           [uk.org.ogsadai.dqp.common.simple
            SimpleDataDictionary SimpleTableSchema]
           [uk.org.ogsadai.converters.databaseschema
            TableMetaDataImpl ColumnMetaDataImpl]
           [uk.org.ogsadai.dqp.presentation.common
            SimpleDataNode SimpleEvaluationNode]))

(defn mock-data-node
  [url
   execution-resource-id
   data-source-service-name
   data-sink-service-name
   dqp-resource-id
   is-local?]
  (SimpleDataNode. url
                   execution-resource-id
                   data-source-service-name
                   data-sink-service-name
                   dqp-resource-id
                   is-local?))

;;; apparently I might need to use different names
;;; with the STS ctor and the TMDI ctor...
(defn mock-table
  ([data-node name cols]
     (mock-table data-node name name cols))
  ([data-node schema-name meta-name cols]
     (SimpleTableSchema.
      (let [table (TableMetaDataImpl. "" "" meta-name)]
        (.setColumns
         table
         (into-array
          (for [[[n tt dt & [pk]] i] (map vector cols (map inc (range)))]
            (doto (ColumnMetaDataImpl. n i table)
              (.setDataType (data-types dt))
              (.setTupleType (tuple-types tt))
              (.setPrimaryKey (boolean pk))))))
        table)
      ^String schema-name
      ^DataNode data-node)))

;;; TODO: do I need a FunctionRepository?

;;; see uk.org.ogsadai.dqp.lqp.LQPTestUtils
(defn mock-data-dictionary [tables]
  (doto (SimpleDataDictionary.)
    (->> repeat
         (map #(.add %2 ^SimpleTableSchema %1) tables)
         dorun)))

(def *mocks* (atom {}))

(defmacro defmock [mock-type mock-name & args]
  `(swap! *mocks* update-in [~mock-type]
          #(if %1 (conj %1 %2) #{%2})
          @(def ~mock-name
                (~(symbol (str "mock-" (name mock-type))) ~@args))))

(defmock :data-node mock-node-1
  "http://host1/"
  "DRER"
  "DSoS"
  "DSiS"
  "Resource1"
  true)

(defmock :data-node mock-node-2-r2
  "http://host2/"
  "DRER"
  "DSoS"
  "DSiS"
  "Resource2"
  false)

(defmock :data-node mock-node-2-r3
  "http://host2/"
  "DRER"
  "DSoS"
  "DSiS"
  "Resource3"
  false)

(defmock :table mock-table-authors
  mock-node-1
  "authors"
  [["name" :string :varchar]
   ["id" :int :integer true]])

(defmock :table mock-table-authors-titles
  mock-node-2-r2
  "authors_titles"
  [["name" :string :varchar]
   ["author_id" :int :integer true]])

(defmock :table mock-table-employee
  mock-node-2-r2
  "l_employee"
  "employee"
  [["fname" :string :varchar]
   ["minit" :char :char]
   ["lname" :string :varchar]
   ["ssn" :string :varchar true]
   ["bdate" :date :date]
   ["address" :string :varchar]
   ["sex" :char :char]
   ["salary" :bigdecimal :decimal]
   ["superssn" :string :varchar true]
   ["dno" :int :integer]])

(defmock :table mock-table-employee-r3
  mock-node-2-r3
  "l_employee_r3"
  "employee_r3"
  [["fname" :string :varchar]
   ["minit" :char :char]
   ["lname" :string :varchar]
   ["ssn" :string :varchar true]
   ["bdate" :date :date]
   ["address" :string :varchar]
   ["sex" :char :char]
   ["salary" :bigdecimal :decimal]
   ["superssn" :string :varchar true]
   ["dno" :int :integer]])

(defmock :table mock-table-department
  mock-node-2-r2
  "l_department"
  "department"
  [["dname" :string :varchar]
   ["dnumber" :int :integer true]
   ["mgrssn" :string :varchar true]
   ["mgrstartdate" :date :date]])

(defmock :table mock-table-dept-locations
  mock-node-2-r2
  "l_dept_locations"
  "dept_locations"
  [["dnumber" :int :integer true]
   ["dlocation" :string :varchar]])

(defmock :table mock-table-project
  mock-node-2-r2
  "l_project"
  "project"
  [["pname" :string :varchar]
   ["pnumber" :int :integer true]
   ["plocation" :string :varchar]
   ["dnum" :int :integer]])

(defmock :table mock-table-works-on
  mock-node-2-r2
  "l_works_on"
  "works_on"
  [["essn" :string :varchar true]
   ["pno" :int :integer true]
   ["hours" :bigdecimal :decimal]])

(defmock :table mock-table-dependent
  mock-node-2-r2
  "dependent"
  [["essn" :string :varchar true]
   ["dependent_name" :string :varchar]
   ["sex" :char :char]
   ["bdate" :date :date]
   ["relationship" :string :varchar]])

(defmock :table mock-table-dependent-r3
  mock-node-2-r3
  "dependent"
  "dependent_r3"
  [["essn" :string :varchar true]
   ["dependent_name" :string :varchar]
   ["sex" :char :char]
   ["bdate" :date :date]
   ["relationship" :string :varchar]])

(defmock :table mock-table-aircraft
  mock-node-2-r2
  "aircraft"
  [["aid" :bigdecimal :decimal true]
   ["aname" :string :varchar]
   ["cruisingrange" :bigdecimal :decimal]])

(defmock :table mock-table-certified
  mock-node-2-r2
  "certified"
  [["eid" :bigdecimal :decimal true]
   ["aid" :bigdecimal :decimal true]])

(defmock :table mock-table-flights
  mock-node-2-r2
  "flights"
  [["flno" :bigdecimal :decimal true]
   ["origin" :string :varchar]
   ["destination" :string :varchar]
   ["distance" :bigdecimal :decimal]
   ["departs" :date :date]
   ["arrives" :date :date]
   ["price" :bigdecimal :decimal]])

(defmock :table mock-table-employees
  mock-node-2-r2
  "employees"
  [["eid" :bigdecimal :decimal true]
   ["ename" :string :varchar]
   ["salary" :bigdecimal :decimal]])

(defmock :table mock-table-series
  mock-node-1
  "series"
  [["name" :string :varchar]
   ["authorname" :string :varchar]
   ["sid" :int :integer true]
   ["date" :string :varchar]])

(defmock :table mock-table-timestamp-table
  mock-node-1
  "timestampTable"
  [["ts" :timestamp :timestamp]
   ["id" :int :integer]])

(defmock :data-dictionary mock-data-dictionary-1
  (:table @*mocks*))

(deftest test-attribute-equality-predicate?
  (are [result sql]
       (= result (attribute-equality-predicate? (parse-condition sql)))
       true "foo = bar"
       true "foo.id = bar.foo"
       false "foo = bar + baz"
       false "foo + bar = baz"))

(def mock-scan-operator-authors
     (let [table-schema (get-table-schema mock-data-dictionary-1
                                          "authors")]
       (make-table-scan-operator
        (:attributes table-schema)
        table-schema
        nil)))

(def mock-scan-operator-authors-titles
     (let [table-schema (get-table-schema mock-data-dictionary-1
                                          "authors_titles")]
       (make-table-scan-operator
        (:attributes table-schema)
        table-schema
        nil)))

(deftest test-predicate-sources
  (is (= (predicate-sources (parse-condition "id = foo.bar")
                            #{mock-scan-operator-authors
                              mock-scan-operator-authors-titles})
         #{mock-scan-operator-authors})))
  