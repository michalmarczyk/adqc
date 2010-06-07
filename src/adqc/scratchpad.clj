(ns adqc.scratchpad)

(comment
  (= (to-sql (-> "f(foo.bar)"
                 antlr-parse-expression
                 antlr->clojure
                 transform-ast))
     "f(foo.bar)"))

#_
(defn parse-sql
  "Parses the given SQL with OGSA-DAI's ANTLR-based parser
  and converts the result to a Clojure tree."
  [sql]
  )

;;; should desperation set in
#_
(defrecord AmbiguousStarExpression [children]
  ToSQL
  (to-sql
   [self]
   (throw (IllegalArgumentException.
           "ambiguous star cannot be represented in SQL")))
  SQLExpression
  (attributes [self] (apply set/union (map attributes children)))
  (rename-attributes
   [self m]
   (AmbiguousStarExpression. (map #(rename-attributes % m) children))))

#_
(let [sql-query-parser (SQLQueryParser/getInstance)]
  
  (defn antlr-parse-sql [sql]
    (.parseSQL sql-query-parser sql))
  
  (defn antlr-parse-condition [sql]
    (.parseSQLForCondition sql-query-parser sql))
  (defn antlr-parse-derived-column [sql]
    (.parseSQLForDerivedColumn sql-query-parser sql))
  (defn antlr-parse-literal [sql]
    (.parseSQLForLiteral sql-query-parser sql))
  (defn antlr-parse-value-expression-or-star [sql]
    (.parseSQLForValueExpressionOrStar sql-query-parser sql))
  (defalias antlr-parse-expression antlr-parse-value-expression-or-star)
  
  (defn antlr-generate-sql [ast]
    (.generateSQL sql-query-parser ast))

  (defn antlr-generate-condition [ast]
    (.generateSQLForCondition sql-query-parser ast))
  (defn antlr-generate-query [ast]
    (.generateSQLForQuery sql-query-parser ast))
  (defn antlr-generate-value-expression-or-star [ast]
    (.generateSQLForValueExpressionOrStar sql-query-parser ast)))

#_
(defmethod transform-node ::antlr-function
  [{[f & args] :children}]
  (FunctionApplicationExpression. f args))

#_
(defmethod transform-node ::antlr-tablecolumn
  [{children :children}]
  (if (next children)
    (Attribute. (second children) (first children) nil)
    (Attribute. (first children) nil nil)))

#_
(defmethod transform-node ::antlr-infix-plus
  [{[x y] :children}]
  (InfixOperatorExpression. (InfixOperator. "+") x y))

#_
(defmethod transform-node ::antlr-infix-minus
  [{[x y] :children}]
  (InfixOperatorExpression. (InfixOperator. "-") x y))

#_
(defmethod transform-node ::antlr-infix-div
  [{[x y] :children}]
  (InfixOperatorExpression. (InfixOperator. "/") x y))

;;; I could add an extra "context" argument to the transform-node
;;; multimethod; it could be used to disambiguate stars etc.
