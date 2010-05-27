(ns adqc.sql
  (:use [adqc.util :only [defextractors extract-info]]
        [clojure
         [zip :as zip :only []]
         [walk :as walk :only []]
         [set :as set :only []]]
        [clojure.contrib
         [string :as str :only []]
         [def :only [defalias]]])
  (:import uk.org.ogsadai.parser.sql92query.SQLQueryParser
           org.antlr.runtime.tree.CommonTree
           org.antlr.runtime.CommonToken))

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

(defn antlr-ast-zip
  [root]
  (zip/zipper (constantly true)
              (fn children [node]
                (let [cnt (.getChildCount ^CommonTree node)]
                  (map #(.getChild ^CommonTree node %) (range cnt))))
              ;; this is wrong, make-node should return
              ;; a *new* branch node
              #_
              (fn make-node [node ^java.util.Collection children]
                (.addChildren ^CommonTree node
                              (java.util.ArrayList. children)))
              (fn make-node [node children]
                (throw
                 (Exception.
                  "adding nodes to ANTLR AST zipper not supported")))
              root))

(defprotocol ANTLR->Clojure
  (antlr->clojure [self]))

(defextractors token-info-extractors CommonToken
  :ttype :type
  :text :text)

(defn node-children [^CommonTree node]
  (let [cnt (.getChildCount node)]
    (loop [i 0 children []]
      (if (< i cnt)
        (recur (inc i)
               (conj children
                     (-> (.getChild node i)
                         antlr->clojure)))
        children))))

(defextractors node-info-extractors CommonTree
  :ntype :type
  :nkey (->> :text (str/replace-re #"_" "-") (str/lower-case) keyword)
  :token (-> :token antlr->clojure)
  :text :text
  :children node-children)

(extend-protocol ANTLR->Clojure
  CommonTree
  (antlr->clojure [self] (extract-info node-info-extractors self))
  CommonToken
  (antlr->clojure [self] (extract-info token-info-extractors self)))

(defprotocol ToSQL
  (to-sql [self]))

(extend-protocol ToSQL
  String
  (to-sql [self] (str/escape char-escape-string self)) ; inadequate...?
  Number
  (to-sql [self] (str self)))

(defprotocol SQLExpression
  (attributes [self])
  (rename-attributes [self m]))

(defrecord InfixOperator [n]
  ToSQL
  (to-sql [self] n))

(defrecord InfixOperatorExpression [op x y]
  ToSQL
  (to-sql [self] (apply str (interpose " " (map to-sql x op y))))
  SQLExpression
  (attributes
   [self]
   (set/union (attributes x)
              (attributes y)))
  (rename-attributes
   [self m]
   (InfixOperatorExpression.
    op
    (rename-attributes x m)
    (rename-attributes y m))))

(defrecord FunctionApplicationExpression [fn args]
  ToSQL
  (to-sql
   [self]
   (str (to-sql fn)
        "(" (apply str (interpose ", " (map to-sql args))) ")"))
  SQLExpression
  (attributes [self] (apply set/union (map attributes args)))
  (rename-attributes
   [self m]
   (vec (map (partial rename-attributes m) args))))

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

;;; this strikes me as less then satisfactory
(def node-type-numbers
     {15 ::antlr-function
      18 ::antlr-tablecolumn
      30 ::antlr-id
      31 ::antlr-int
      49 ::antlr-star
      65 ::antlr-infix-plus
      66 ::antlr-infix-minus
      67 ::antlr-infix-div})

(defmulti transform-node (comp node-type-numbers :ntype))

(defmacro deftn [dispatch-val children-pat & body]
  `(defmethod transform-node ~dispatch-val
     [{~children-pat :children}]
     ~@body))

(defmethod transform-node nil [token] token)

(deftn ::antlr-function [f & args]
  (FunctionApplicationExpression. f args))

#_
(defmethod transform-node ::antlr-function
  [{[f & args] :children}]
  (FunctionApplicationExpression. f args))

;;; do I want to haul some context around
;;; (to be queried for the source of an attribute etc.)?
(deftn ::antlr-tablecolumn children
  (if (next children)
    (Attribute. (second children) (first children) nil)
    (Attribute. (first children) nil nil)))

#_
(defmethod transform-node ::antlr-tablecolumn
  [{children :children}]
  (if (next children)
    (Attribute. (second children) (first children) nil)
    (Attribute. (first children) nil nil)))

(defmacro deftin [dispatch-val op]
  `(deftn ~dispatch-val [x# y#]
     (InfixOperatorExpression. (InfixOperator. ~(str op)) x# y#)))

(deftin ::antlr-infix-plus +)
(deftin ::antlr-infix-minus -)
(deftin ::antlr-infix-div /)

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

(defmethod transform-node ::antlr-id [{text :text}] text)
(defmethod transform-node ::antlr-int [{text :text}] (BigInteger. text))

;;; Clojure maps and vectors will only occur here where antlr->clojure
;;; constructs them, so I'm free to use map? and vector? to determine
;;; whether I'm changing something in postwalk.
(defn transform-ast [ast]
  (walk/postwalk
   (fn [item]
     (cond (map? item) (transform-node item)
           ;; vec necessary (because clojure.lang.MapEntry satisfies vector?)
           ;; -- I wonder if there is potential for breakage that I can't see
           (vector? item) (vec (map transform-node item))
           :else item))
   ast))

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
