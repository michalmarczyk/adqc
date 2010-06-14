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

;;; original defrecord forms...

#_
(defrecord InfixOperatorExpression [op lhs rhs]
  ToSQL
  (to-sql [self] (apply str (interpose " " (map to-sql [lhs op rhs]))))
  SQLExpression
  (attributes
   [self]
   (set/union (attributes lhs)
              (attributes rhs)))
  (rename-attributes
   [self m]
   (InfixOperatorExpression.
    op
    (rename-attributes lhs m)
    (rename-attributes rhs m))))

#_
(defrecord FunctionApplicationExpression [fn args]
  ToSQL
  (to-sql
   [self]
   (str fn "(" (apply str (interpose ", " (map to-sql args))) ")"))
  SQLExpression
  (attributes [self] (apply set/union (map attributes args)))
  (rename-attributes
   [self m]
   (vec (map (partial rename-attributes m) args))))

#_
(defrecord Attribute [id src t]
  ToSQL
  (to-sql
   [self]
   (if src (str src "." id) id))
  SQLExpression
  (attributes [self] #{self})
  (rename-attributes
   [self m]
   (if-let [new-id (m id)]
     (Attribute. new-id src t)
     self)))

#_
(defrecord ColumnStar []
  ToSQL
  (to-sql [self] "*")
  SQLExpression
  (attributes [self] #{})
  (rename-attributes [self _] self))

#_
(defrecord InfixPredicateExpression [pred lhs rhs]
  ToSQL
  (to-sql [self] (apply str (interpose " " (map to-sql [lhs pred rhs]))))
  SQLExpression
  (attributes [self] (set/union (attributes lhs) (attributes rhs)))
  (rename-attributes
   [self m]
   (InfixPredicateExpression.
    pred
    (rename-attributes lhs m)
    (rename-attributes rhs m))))

#_
(defrecord PredicateConjunctionExpression [lhs rhs]
  ToSQL
  (to-sql [self] (apply str (interpose " " (map parenthesise [lhs "AND" rhs]))))
  SQLExpression
  (attributes [self] (set/union (attributes lhs) (attributes rhs)))
  (rename-attributes
   [self m]
   (PredicateConjunctionExpression. (rename-attributes lhs m)
                                    (rename-attributes rhs m))))

#_
(defrecord PredicateDisjunctionExpression [lhs rhs]
  ToSQL
  (to-sql [self] (apply str (interpose " " (map parenthesise [lhs "OR" rhs]))))
  SQLExpression
  (attributes [self] (set/union (attributes lhs) (attributes rhs)))
  (rename-attributes
   [self m]
   (PredicateDisjunctionExpression. (rename-attributes lhs m)
                                    (rename-attributes rhs m))))

#_
(defrecord PredicateNegationExpression [arg]
  ToSQL
  (to-sql [self] (str "NOT " (parenthesise arg)))
  SQLExpression
  (attributes [self] (attributes arg))
  (rename-attributes
   [self m]
   (PredicateNegationExpression. (rename-attributes arg m))))

#_
(defrecord IsNullExpression [col]
  ToSQL
  (to-sql [self] (str (to-sql col) " IS NULL"))
  SQLExpression
  (attributes [self] (attributes col))
  (rename-attributes
   [self m]
   (IsNullExpression. (rename-attributes col m))))
