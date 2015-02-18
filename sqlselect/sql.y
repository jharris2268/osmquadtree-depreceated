// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

%{
package sqlselect
import "strings"

%}

%union {
  empty     struct{}
  str       string
  Tablers   []Tabler
  Tabler    Tabler
  Rowers    []Rower
  Rower     Rower
  Wherer    Wherer
  Values    []Value
  Value     Value
  ValueTuples [][]Value
  Row       Row
  caseRower *caseRower
  whens     []exPair
  when      exPair
  
  orderList orderList
  order     order
  
  insRows   *valuesTable
  
}

%token LEX_ERROR
%token <empty> SELECT FROM WHERE ORDER BY
%token <empty> ALL DISTINCT AS IN IS LIKE BETWEEN NULL ASC DESC VALUES
%token <str> ID STRING NUMBER VALUE_ARG LIST_ARG
%token <empty> LE GE NE NULL_SAFE_EQUAL
%token <empty> '(' EQ GT LT

%left <empty> UNION USING IF
%left <empty> ','
%left <str> JOIN
%left <empty> ON
%left <empty> OR
%left <empty> AND
%right <empty> NOT
%left <empty> BR
%left <empty> PL MN
%left <empty> ST DV
%nonassoc <empty> '.'
%left <empty> UNARY
%right <empty> CASE, WHEN, THEN, ELSE
%left <empty> END

%start any_command

%type <Tabler> select_statement
%type <Rowers> select_expr
%type <Rowers> select_expression_list
%type <Rower> select_expression
%type <str> as_lower_opt as_opt
%type <Rower> expression
//%type <Tablers> table_expression_list
%type <Tabler> table_expression
%type <str> join_type
%type <Tabler> simple_table_expression
%type <Wherer> where_expression_opt
%type <Wherer> boolean_expression condition
%type <str> compare
%type <Value> value
%type <Rower> value_expression
%type <Rowers> col_tuple
//%type <Rowers> value_expression_list
%type <ValueTuples> tuple_list
%type <Values> row_tuple
//%type <str> keyword_as_func
%type <Tabler> subquery
//%type <str> unary_operator
%type <Rower> column_name
%type <caseRower> case_expression
%type <whens> when_expression_list
%type <when> when_expression
%type <Rower> /*value_expression_opt*/ else_expression_opt
%type <orderList> order_by_opt order_list
%type <str> asc_desc_opt
%type <Rowers> column_list_opt column_list
%type <str> sql_id
%type <order> order
%type <Values> value_list


%%

any_command:
  select_statement
  {
    yylex.(*LexResult).result = $1
    return 0  
  }
  
  ;

select_statement:
  SELECT select_expr FROM table_expression where_expression_opt order_by_opt
  {
    $$ = makeSimpleSelect($4,$2,$5,$6)
  }
| select_statement UNION select_statement
  {
    $$ = &unionQuery{$1,$3,"",""}
  }
| select_statement UNION ALL select_statement
  {
    $$ = &unionQuery{$1,$4,"",""}
  }
| '(' select_statement ')'
  {
    $$ = $2
  }


select_expr:
    ST
    {
        $$ = nil
    }
|   
    select_expression_list
    {
        $$ = $1
    }


select_expression_list:
  select_expression
  {
    $$ = rowerList{$1}
  }
| select_expression_list ',' select_expression
  {
    $$ = append($$, $3)
  }

select_expression:
 expression as_lower_opt
  {
    $$ = isAsExpr($1,$2)
  }

expression:
  boolean_expression
  {
    $$ = &whereRower{$1}
  }
| value_expression
  {
    $$ = $1
  }

as_lower_opt:
  {
    $$ = ""
  }
| sql_id
  {
    $$ = $1
  }
| AS sql_id
  {
    $$ = $2
  }
/*
table_expression_list:
  table_expression
  {
    $$ = []Tabler{$1}
  }
| table_expression_list ',' table_expression
  {
    $$ = append($$, $3)
  }
*/
table_expression:
  simple_table_expression as_opt
  {
    $$ = isAsTable($1,$2)
  }
| '(' table_expression ')'
  {
    $$ = $2
  }
| table_expression join_type table_expression USING column_list_opt %prec JOIN
  {
    $$ = &joinQuery{$1, $3, $5}
  }
| '(' VALUES tuple_list ')' as_opt column_list_opt
  {
    $$ = makeValuesTable($3,$6)
  }

as_opt:
  {
    $$ = ""
  }
| ID
  {
    $$ = $1
  }
| AS ID
  {
    $$ = $2
  }

join_type:
  JOIN
  {
    $$ = "JOIN"
  }
  

simple_table_expression:
ID
  {
    $$ = pickTable($1)
  }
| ID '.' ID
  {
    $$ = pickTable($3)
  }
| subquery
  {
    $$ = $1
  }



where_expression_opt:
  {
    $$ = nil
  }
| WHERE boolean_expression
  {
    $$ = $2
  }

boolean_expression:
  condition
| boolean_expression AND boolean_expression
  {
    $$ = &andExpr{$1, $3}
  }
| boolean_expression OR boolean_expression
  {
    $$ = &orExpr{$1, $3}
  }
| NOT boolean_expression
  {
    $$ = &notExpr{$2}
  }
| '(' boolean_expression ')'
  {
    $$ = $2
  }

condition:
  value_expression compare value_expression
  {
    $$ = &compExpr{$1, $3, $2}
  }
| value_expression IN col_tuple
  {
    $$ = &isInExpr{$1,$3}
  }
| value_expression NOT IN col_tuple
  {
    $$ = &notExpr{&isInExpr{$1, $4} }
  }
/*| value_expression LIKE value_expression
  {
    $$ = &ComparisonExpr{Left: $1, Operator: AST_LIKE, Right: $3}
  }
| value_expression NOT LIKE value_expression
  {
    $$ = &ComparisonExpr{Left: $1, Operator: AST_NOT_LIKE, Right: $4}
  }*/
| value_expression BETWEEN value_expression AND value_expression
  {
    $$ = &rangeExpr{$1, $3, $5}
  }
| value_expression NOT BETWEEN value_expression AND value_expression
  {
    $$ = &notExpr{&rangeExpr{$1, $4, $6}}
  }
| value_expression IS NULL
  {
    $$ = &isNullExpr{$1}
  }
| value_expression IS NOT NULL
  {
    $$ = &notExpr{&isNullExpr{$1}}
  }
/*| EXISTS subquery
  {
    $$ = &ExistsExpr{Subquery: $2}
  }
*/
compare:
  EQ
  {
    $$ = "="
  }
| LT
  {
    $$ = "<"
  }
| GT
  {
    $$ = ">"
  }
| LE
  {
    $$ = "<="
  }
| GE
  {
    $$ = ">="
  }
| NE
  {
    $$ = "!="
  }

col_tuple:
  '(' select_expression_list ')'
  {
    $$ = $2
  }
/*| subquery
  {
    $$ = $1
  }
| LIST_ARG
  {
    $$ = ListArg($1)
  }*/

subquery:
  '(' select_statement ')'
  {
    $$ = $2
  }

value_list:
  value
  {
    $$ = []Value{$1}
  }
| value_list ',' value
  {
    $$ = append($1, $3)
  }

value_expression:

column_name
  {
    $$ = $1
  }
|  value
  {
    $$ = &valRow{"",$1}
  }
| '(' value_expression ')'
  {
    $$ = $2
  }
/*| row_tuple
  {
    $$ = $1 //[]Rower{$1}
  }
| value_expression '&' value_expression
  {
    $$ = &BinaryExpr{Left: $1, Operator: AST_BITAND, Right: $3}
  }*/
| value_expression BR value_expression
  {
    $$ = &concatExpr{$1, $3}
  }
/*| value_expression '^' value_expression
  {
    $$ = &binaryExpr{Left: $1, Operator: AST_BITXOR, Right: $3}
  }*/
| value_expression PL value_expression
  {
    $$ = &mathExpr{$1, $3, "+"}
  }
| value_expression MN value_expression
  {
    $$ = &mathExpr{$1, $3, "-"}
  }
| value_expression ST value_expression
  {
    $$ = &mathExpr{$1, $3, "*"}
  }
| value_expression DV value_expression
  {
    $$ = &mathExpr{$1, $3, "/"}
  }
/*| value_expression '%' value_expression
  {
    $$ = &BinaryExpr{Left: $1, Operator: AST_MOD, Right: $3}
  }
| unary_operator value_expression %prec UNARY
  {
    if num, ok := $2.(NumVal); ok {
      switch $1 {
      case '-':
        $$ = append(NumVal("-"), num...)
      case '+':
        $$ = num
      default:
        $$ = &UnaryExpr{Operator: $1, Expr: $2}
      }
    } else {
      $$ = &UnaryExpr{Operator: $1, Expr: $2}
    }
  }
| sql_id '(' ')'
  {
    $$ = &funcExpr{$1}
  }
| sql_id '(' select_expression_list ')'
  {
    $$ = &funcExpr{$1, $3}
  }
| sql_id '(' DISTINCT select_expression_list ')'
  {
    $$ = &FuncExpr{Name: $1, Distinct: true, Exprs: $4}
  }*/
| sql_id '(' select_expression_list ')'
  {
    $$ = &funcExpr{$1, $3}
  }
| case_expression
  {
    $$ = $1
  }
/*
keyword_as_func:
  IF
  {
    $$ = IF_BYTES
  }
| VALUES
  {
    $$ = VALUES_BYTES
  }
*/
/*unary_operator:
  '+'
  {
    $$ = AST_UPLUS
  }
| '-'
  {
    $$ = AST_UMINUS
  }
| '~'
  {
    $$ = AST_TILDA
  }
*/
case_expression:
  CASE when_expression_list else_expression_opt END
  {
    $$ = &caseRower{$2, $3}
  }

/*value_expression_opt:
  {
    $$ = nil
  }
| value_expression
  {
    $$ = $1
  }
*/

when_expression_list:
  when_expression
  {
    $$ = []exPair{$1}
  }
| when_expression_list when_expression
  {
    $$ = append($1, $2)
  }

when_expression:
  WHEN boolean_expression THEN value_expression
  {
    $$ = exPair{$2, $4}
  }

else_expression_opt:
  {
    $$ = nil
  }
| ELSE value_expression
  {
    $$ = $2
  }

column_name:
  sql_id
  {
    $$ = pickRow($1)
  }
| ID '.' sql_id
  {
    $$ = pickRow($1)//&ColName{Qualifier: $1, Name: $3}
  }

value:
  STRING
  {
    $$ = stringValue($1)
  }
| NUMBER
  {
    $$ = makeNumVal($1)
  }
| VALUE_ARG
  {
    $$ = makeNumVal($1)
  }
| NULL
  {
    $$ = nullValue()
  }


order_by_opt:
  {
    $$ = nil
  }
| ORDER BY order_list
  {
    $$ = $3
  }

order_list:
  order
  {
    $$ = orderList{$1}
  }
| order_list ',' order
  {
    $$ = append($1, $3)
  }

order:
  value_expression asc_desc_opt
  {
    $$ = order{$1, $2}
  }

asc_desc_opt:
  {
    $$ = "ASC"
  }
| ASC
  {
    $$ = "ASC"
  }
| DESC
  {
    $$ = "DESC"
  }


column_list_opt:
  {
    $$ = nil
  }
| '(' column_list ')'
  {
    $$ = $2
  }

column_list:
  column_name
  {
    $$ = []Rower{$1}
  }
| column_list ',' column_name
  {
    $$ = append($$, $3)
  }


tuple_list:
  row_tuple
  {
    $$ = [][]Value{$1}
  }
| tuple_list ',' row_tuple
  {
    $$ = append($1, $3)
  }

row_tuple:
  '(' value_list ')'
  {
    $$ = []Value($2)
  }
/*| subquery
  {
    $$ = $1
  }
*/

sql_id:
  ID
  {
    $$ = strings.ToLower($1)
  }
/*
force_eof:
{
  ForceEOF(yylex)
}
*/
