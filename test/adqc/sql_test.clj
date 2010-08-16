(ns adqc.sql-test
  (:use clojure.test)
  (:use [adqc sql]
        [adqc.sql records protocols]))

(deftest test-roundtripping-arithmetic-expressions
  (are [sql]
       (= (vec (.split #"\s+" sql))
          (vec (.split #"\s+" (-> sql
                                  parse-expression
                                  to-sql))))
       "x + y"
       "x - y"
       "x * y"
       "x / y"
       "f(foo.bar, 3 * 2) + quux"))

(deftest test-roundtripping-predicate-expressions
  (are [sql]
       (= (vec (.split #"\s+" sql))
          (vec (.split #"\s+" (-> sql
                                  parse-condition
                                  to-sql))))
       "x < y"
       "x <= y"
       "x > y"
       "x >= y"
       "x = y"
       "x != y"
       "x <> y"
       "foo LIKE '%bar'"
       "foo < bar AND baz > quux"
       "foo < bar OR bar > quux"
       "(foo LIKE '%bar' OR foo LIKE '%quux') AND (NOT wibble < wobble)"
       "foo.bar IS NULL"))

;;; NB. a correct implementation could fail this test (e.g. due to
;;; different paren placement), but for now it's a simple way to check
;;; if things work the way I think they do.
(deftest test-roundtrip-statements
  (are [sql]
       (= (vec (.split #"\s+" sql))
          (vec (.split #"\s+" (-> sql parse-sql to-sql))))
       "SELECT foo AS f FROM bar AS b WHERE f < 1000"))

(deftest test-attributes
  (are [expr attrs]
       (= (-> expr parse-expression attributes) attrs)
       "foo" #{(make-attribute "foo" nil nil)}
       "foo.bar + quux" #{(make-attribute "bar" "foo" nil)
                          (make-attribute "quux" nil nil)})
  (are [pred attrs]
       (= (-> pred parse-condition attributes) attrs)
       "foo.bar < quux" #{(make-attribute "bar" "foo" nil)
                          (make-attribute "quux" nil nil)}
       "foo < bar AND baz.quux LIKE '%asdf'"
       #{(make-attribute "foo" nil nil)
         (make-attribute "bar" nil nil)
         (make-attribute "quux" "baz" nil)}))

(deftest test-rename-attributes
  (are [expr expr* m]
       (= (vec (.split #"\s+" expr*))
          (-> expr
              parse-expression
              (rename-attributes m)
              to-sql
              (->> (.split #"\s+"))
              vec))
       "foo" "bar" {"foo" "bar"}
       "quux.foo" "quux.bar" {"foo" "bar"}))
