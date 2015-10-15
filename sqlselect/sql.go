//line sql.y:9
package sqlselect

import __yyfmt__ "fmt"

//line sql.y:9
import "strings"

//line sql.y:14
type yySymType struct {
	yys         int
	empty       struct{}
	str         string
	Tablers     []Tabler
	Tabler      Tabler
	Rowers      []Rower
	Rower       Rower
	Wherer      Wherer
	Values      []Value
	Value       Value
	ValueTuples [][]Value
	Row         Row
	caseRower   *caseRower
	whens       []exPair
	when        exPair

	orderList orderList
	order     order

	insRows *valuesTable
}

const LEX_ERROR = 57346
const SELECT = 57347
const FROM = 57348
const WHERE = 57349
const ORDER = 57350
const BY = 57351
const ALL = 57352
const DISTINCT = 57353
const AS = 57354
const IN = 57355
const IS = 57356
const LIKE = 57357
const BETWEEN = 57358
const NULL = 57359
const ASC = 57360
const DESC = 57361
const VALUES = 57362
const ID = 57363
const STRING = 57364
const NUMBER = 57365
const VALUE_ARG = 57366
const LIST_ARG = 57367
const LE = 57368
const GE = 57369
const NE = 57370
const NULL_SAFE_EQUAL = 57371
const EQ = 57372
const GT = 57373
const LT = 57374
const UNION = 57375
const USING = 57376
const IF = 57377
const JOIN = 57378
const ON = 57379
const OR = 57380
const AND = 57381
const NOT = 57382
const BR = 57383
const PL = 57384
const MN = 57385
const ST = 57386
const DV = 57387
const UNARY = 57388
const CASE = 57389
const WHEN = 57390
const THEN = 57391
const ELSE = 57392
const END = 57393

var yyToknames = []string{
	"LEX_ERROR",
	"SELECT",
	"FROM",
	"WHERE",
	"ORDER",
	"BY",
	"ALL",
	"DISTINCT",
	"AS",
	"IN",
	"IS",
	"LIKE",
	"BETWEEN",
	"NULL",
	"ASC",
	"DESC",
	"VALUES",
	"ID",
	"STRING",
	"NUMBER",
	"VALUE_ARG",
	"LIST_ARG",
	"LE",
	"GE",
	"NE",
	"NULL_SAFE_EQUAL",
	"'('",
	"EQ",
	"GT",
	"LT",
	"UNION",
	"USING",
	"IF",
	"','",
	"JOIN",
	"ON",
	"OR",
	"AND",
	"NOT",
	"BR",
	"PL",
	"MN",
	"ST",
	"DV",
	"'.'",
	"UNARY",
	"CASE",
	"WHEN",
	"THEN",
	"ELSE",
	"END",
}
var yyStatenames = []string{}

const yyEofCode = 1
const yyErrCode = 2
const yyMaxDepth = 200

//line yacctab:1
var yyExca = []int{
	-1, 1,
	1, -1,
	-2, 0,
	-1, 143,
	34, 5,
	55, 5,
	-2, 50,
}

const yyNprod = 91
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 256

var yyAct = []int{

	55, 18, 16, 146, 12, 148, 17, 128, 76, 102,
	105, 8, 32, 61, 2, 165, 57, 36, 35, 26,
	27, 11, 43, 44, 45, 46, 47, 118, 5, 46,
	47, 12, 90, 164, 91, 71, 54, 56, 74, 101,
	65, 81, 82, 64, 85, 86, 87, 88, 89, 143,
	152, 140, 35, 30, 5, 62, 126, 72, 73, 12,
	24, 93, 5, 109, 20, 21, 22, 23, 153, 139,
	92, 133, 30, 15, 95, 130, 110, 62, 12, 96,
	113, 114, 107, 63, 97, 14, 59, 112, 5, 111,
	117, 36, 35, 25, 154, 83, 30, 119, 149, 38,
	42, 40, 41, 120, 43, 44, 45, 46, 47, 129,
	123, 77, 51, 52, 53, 58, 135, 48, 50, 49,
	84, 136, 124, 131, 44, 45, 46, 47, 39, 43,
	44, 45, 46, 47, 100, 144, 142, 20, 147, 3,
	134, 91, 43, 44, 45, 46, 47, 138, 151, 150,
	101, 160, 159, 36, 35, 147, 161, 68, 163, 162,
	38, 42, 40, 41, 4, 101, 67, 160, 166, 132,
	125, 3, 9, 51, 52, 53, 28, 34, 48, 50,
	49, 115, 3, 43, 44, 45, 46, 47, 116, 39,
	43, 44, 45, 46, 47, 24, 4, 106, 68, 20,
	21, 22, 23, 70, 104, 24, 137, 108, 15, 20,
	21, 22, 23, 103, 156, 157, 24, 33, 75, 122,
	14, 21, 22, 23, 7, 29, 34, 78, 25, 79,
	80, 141, 158, 155, 145, 121, 94, 60, 25, 43,
	44, 45, 46, 47, 19, 69, 127, 37, 13, 98,
	66, 99, 10, 31, 6, 1,
}
var yyPact = []int{

	134, -1000, 54, 178, 134, 166, 219, -1000, 59, -1000,
	205, 113, 147, -1000, 43, 43, -1000, -1000, 85, -1000,
	38, -1000, -1000, -1000, -1000, 4, 28, -1000, 134, 136,
	43, -1000, -1000, 156, -1000, 43, 43, 188, 81, 214,
	188, 188, 78, 188, 188, 188, 188, 188, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, 147, -23, 86, 43, 156,
	26, -1000, 43, -1000, 54, 127, 192, 177, 15, -1000,
	-1000, -1000, -1000, 11, 61, 188, -1000, 43, 81, 188,
	188, 61, 140, -1000, 171, 80, -17, -17, -1000, -1000,
	-1000, -1000, 35, -1000, -27, -1000, 188, 51, 211, 136,
	43, -1000, -1000, -1000, 149, 1, 79, 20, 177, 148,
	-21, 16, -1000, 61, 99, 188, -1000, -1000, -1000, 61,
	188, -1000, 197, 112, 113, -1000, -1000, 14, -1000, 199,
	-1000, -6, -1000, -1000, 188, 61, 61, 188, 68, 192,
	79, 13, -1000, -1000, 61, 57, -1000, 196, -1000, 116,
	68, -1000, 199, -1000, 188, -1000, -1000, -1000, -22, -1000,
	-1000, -1000, -1000, -1000, -1000, 116, -1000,
}
var yyPgo = []int{

	0, 255, 14, 254, 11, 172, 253, 9, 252, 10,
	251, 250, 249, 21, 248, 247, 6, 0, 8, 246,
	7, 245, 2, 244, 237, 13, 236, 235, 234, 233,
	5, 232, 1, 3, 231,
}
var yyR1 = []int{

	0, 1, 2, 2, 2, 2, 3, 3, 4, 4,
	5, 8, 8, 6, 6, 6, 9, 9, 9, 9,
	7, 7, 7, 10, 11, 11, 11, 12, 12, 13,
	13, 13, 13, 13, 14, 14, 14, 14, 14, 14,
	14, 14, 14, 15, 15, 15, 15, 15, 15, 18,
	21, 34, 34, 17, 17, 17, 17, 17, 17, 17,
	17, 17, 17, 23, 24, 24, 25, 26, 26, 22,
	22, 16, 16, 16, 16, 27, 27, 28, 28, 33,
	29, 29, 29, 30, 30, 31, 31, 19, 19, 20,
	32,
}
var yyR2 = []int{

	0, 1, 6, 3, 4, 3, 1, 1, 1, 3,
	2, 1, 1, 0, 1, 2, 2, 3, 5, 6,
	0, 1, 2, 1, 1, 3, 1, 0, 2, 1,
	3, 3, 2, 3, 3, 3, 4, 3, 4, 5,
	6, 3, 4, 1, 1, 1, 1, 1, 1, 3,
	3, 1, 3, 1, 1, 3, 3, 3, 3, 3,
	3, 4, 1, 4, 1, 2, 4, 0, 2, 1,
	3, 1, 1, 1, 1, 0, 3, 1, 3, 2,
	0, 1, 1, 0, 3, 1, 3, 1, 3, 3,
	1,
}
var yyChk = []int{

	-1000, -1, -2, 5, 30, 34, -3, 46, -4, -5,
	-8, -13, -17, -14, 42, 30, -22, -16, -32, -23,
	21, 22, 23, 24, 17, 50, -2, -2, 10, 6,
	37, -6, -32, 12, 21, 41, 40, -15, 13, 42,
	15, 16, 14, 43, 44, 45, 46, 47, 31, 33,
	32, 26, 27, 28, -13, -17, -13, -17, 30, 48,
	-24, -25, 51, 55, -2, -9, -11, 30, 21, -21,
	-5, -32, -13, -13, -17, 30, -18, 30, 13, 15,
	16, -17, -17, 17, 42, -17, -17, -17, -17, -17,
	55, 55, -4, -32, -26, -25, 53, -13, -12, -10,
	7, 38, -7, 21, 12, -9, 20, -2, 30, 48,
	-17, -4, -18, -17, -17, 41, 17, 55, 54, -17,
	52, -27, 8, -9, -13, 21, 55, -19, -20, 30,
	55, -2, 21, 55, 41, -17, -17, 9, 35, 55,
	37, -34, -16, 55, -17, -28, -33, -17, -30, 30,
	-7, -20, 37, 55, 37, -29, 18, 19, -31, -22,
	-32, -30, -16, -33, 55, 37, -22,
}
var yyDef = []int{

	0, -2, 1, 0, 0, 0, 0, 6, 7, 8,
	13, 11, 12, 29, 0, 0, 53, 54, 69, 62,
	90, 71, 72, 73, 74, 0, 0, 3, 0, 0,
	0, 10, 14, 0, 90, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 43, 44,
	45, 46, 47, 48, 32, 0, 0, 0, 0, 0,
	67, 64, 0, 5, 4, 27, 20, 0, 24, 26,
	9, 15, 30, 31, 34, 0, 35, 0, 0, 0,
	0, 37, 0, 41, 0, 56, 57, 58, 59, 60,
	33, 55, 0, 70, 0, 65, 0, 0, 75, 0,
	0, 23, 16, 21, 0, 0, 0, 0, 0, 0,
	0, 0, 36, 38, 0, 0, 42, 61, 63, 68,
	0, 2, 0, 0, 28, 22, 17, 0, 87, 0,
	50, 0, 25, 49, 0, 39, 66, 0, 83, 20,
	0, 0, 51, -2, 40, 76, 77, 80, 18, 0,
	83, 88, 0, 89, 0, 79, 81, 82, 0, 85,
	69, 19, 52, 78, 84, 0, 86,
}
var yyTok1 = []int{

	1, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	30, 55, 3, 3, 37, 3, 48,
}
var yyTok2 = []int{

	2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
	12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
	22, 23, 24, 25, 26, 27, 28, 29, 31, 32,
	33, 34, 35, 36, 38, 39, 40, 41, 42, 43,
	44, 45, 46, 47, 49, 50, 51, 52, 53, 54,
}
var yyTok3 = []int{
	0,
}

//line yaccpar:1

/*	parser for yacc output	*/

var yyDebug = 0

type yyLexer interface {
	Lex(lval *yySymType) int
	Error(s string)
}

const yyFlag = -1000

func yyTokname(c int) string {
	// 4 is TOKSTART above
	if c >= 4 && c-4 < len(yyToknames) {
		if yyToknames[c-4] != "" {
			return yyToknames[c-4]
		}
	}
	return __yyfmt__.Sprintf("tok-%v", c)
}

func yyStatname(s int) string {
	if s >= 0 && s < len(yyStatenames) {
		if yyStatenames[s] != "" {
			return yyStatenames[s]
		}
	}
	return __yyfmt__.Sprintf("state-%v", s)
}

func yylex1(lex yyLexer, lval *yySymType) int {
	c := 0
	char := lex.Lex(lval)
	if char <= 0 {
		c = yyTok1[0]
		goto out
	}
	if char < len(yyTok1) {
		c = yyTok1[char]
		goto out
	}
	if char >= yyPrivate {
		if char < yyPrivate+len(yyTok2) {
			c = yyTok2[char-yyPrivate]
			goto out
		}
	}
	for i := 0; i < len(yyTok3); i += 2 {
		c = yyTok3[i+0]
		if c == char {
			c = yyTok3[i+1]
			goto out
		}
	}

out:
	if c == 0 {
		c = yyTok2[1] /* unknown char */
	}
	if yyDebug >= 3 {
		__yyfmt__.Printf("lex %s(%d)\n", yyTokname(c), uint(char))
	}
	return c
}

func yyParse(yylex yyLexer) int {
	var yyn int
	var yylval yySymType
	var yyVAL yySymType
	yyS := make([]yySymType, yyMaxDepth)

	Nerrs := 0   /* number of errors */
	Errflag := 0 /* error recovery flag */
	yystate := 0
	yychar := -1
	yyp := -1
	goto yystack

ret0:
	return 0

ret1:
	return 1

yystack:
	/* put a state and value onto the stack */
	if yyDebug >= 4 {
		__yyfmt__.Printf("char %v in %v\n", yyTokname(yychar), yyStatname(yystate))
	}

	yyp++
	if yyp >= len(yyS) {
		nyys := make([]yySymType, len(yyS)*2)
		copy(nyys, yyS)
		yyS = nyys
	}
	yyS[yyp] = yyVAL
	yyS[yyp].yys = yystate

yynewstate:
	yyn = yyPact[yystate]
	if yyn <= yyFlag {
		goto yydefault /* simple state */
	}
	if yychar < 0 {
		yychar = yylex1(yylex, &yylval)
	}
	yyn += yychar
	if yyn < 0 || yyn >= yyLast {
		goto yydefault
	}
	yyn = yyAct[yyn]
	if yyChk[yyn] == yychar { /* valid shift */
		yychar = -1
		yyVAL = yylval
		yystate = yyn
		if Errflag > 0 {
			Errflag--
		}
		goto yystack
	}

yydefault:
	/* default state action */
	yyn = yyDef[yystate]
	if yyn == -2 {
		if yychar < 0 {
			yychar = yylex1(yylex, &yylval)
		}

		/* look through exception table */
		xi := 0
		for {
			if yyExca[xi+0] == -1 && yyExca[xi+1] == yystate {
				break
			}
			xi += 2
		}
		for xi += 2; ; xi += 2 {
			yyn = yyExca[xi+0]
			if yyn < 0 || yyn == yychar {
				break
			}
		}
		yyn = yyExca[xi+1]
		if yyn < 0 {
			goto ret0
		}
	}
	if yyn == 0 {
		/* error ... attempt to resume parsing */
		switch Errflag {
		case 0: /* brand new error */
			yylex.Error("syntax error")
			Nerrs++
			if yyDebug >= 1 {
				__yyfmt__.Printf("%s", yyStatname(yystate))
				__yyfmt__.Printf(" saw %s\n", yyTokname(yychar))
			}
			fallthrough

		case 1, 2: /* incompletely recovered error ... try again */
			Errflag = 3

			/* find a state where "error" is a legal shift action */
			for yyp >= 0 {
				yyn = yyPact[yyS[yyp].yys] + yyErrCode
				if yyn >= 0 && yyn < yyLast {
					yystate = yyAct[yyn] /* simulate a shift of "error" */
					if yyChk[yystate] == yyErrCode {
						goto yystack
					}
				}

				/* the current p has no shift on "error", pop stack */
				if yyDebug >= 2 {
					__yyfmt__.Printf("error recovery pops state %d\n", yyS[yyp].yys)
				}
				yyp--
			}
			/* there is no state on the stack with an error shift ... abort */
			goto ret1

		case 3: /* no shift yet; clobber input char */
			if yyDebug >= 2 {
				__yyfmt__.Printf("error recovery discards %s\n", yyTokname(yychar))
			}
			if yychar == yyEofCode {
				goto ret1
			}
			yychar = -1
			goto yynewstate /* try again in the same state */
		}
	}

	/* reduction by production yyn */
	if yyDebug >= 2 {
		__yyfmt__.Printf("reduce %v in:\n\t%v\n", yyn, yyStatname(yystate))
	}

	yynt := yyn
	yypt := yyp
	_ = yypt // guard against "declared and not used"

	yyp -= yyR2[yyn]
	yyVAL = yyS[yyp+1]

	/* consult goto table to find next state */
	yyn = yyR1[yyn]
	yyg := yyPgo[yyn]
	yyj := yyg + yyS[yyp].yys + 1

	if yyj >= yyLast {
		yystate = yyAct[yyg]
	} else {
		yystate = yyAct[yyj]
		if yyChk[yystate] != -yyn {
			yystate = yyAct[yyg]
		}
	}
	// dummy call; replaced with literal code
	switch yynt {

	case 1:
		//line sql.y:100
		{
			yylex.(*LexResult).result = yyS[yypt-0].Tabler
			return 0
		}
	case 2:
		//line sql.y:109
		{
			yyVAL.Tabler = makeSimpleSelect(yyS[yypt-2].Tabler, yyS[yypt-4].Rowers, yyS[yypt-1].Wherer, yyS[yypt-0].orderList)
		}
	case 3:
		//line sql.y:113
		{
			yyVAL.Tabler = &unionQuery{yyS[yypt-2].Tabler, yyS[yypt-0].Tabler, "", ""}
		}
	case 4:
		//line sql.y:117
		{
			yyVAL.Tabler = &unionQuery{yyS[yypt-3].Tabler, yyS[yypt-0].Tabler, "", ""}
		}
	case 5:
		//line sql.y:121
		{
			yyVAL.Tabler = yyS[yypt-1].Tabler
		}
	case 6:
		//line sql.y:128
		{
			yyVAL.Rowers = nil
		}
	case 7:
		//line sql.y:133
		{
			yyVAL.Rowers = yyS[yypt-0].Rowers
		}
	case 8:
		//line sql.y:140
		{
			yyVAL.Rowers = rowerList{yyS[yypt-0].Rower}
		}
	case 9:
		//line sql.y:144
		{
			yyVAL.Rowers = append(yyVAL.Rowers, yyS[yypt-0].Rower)
		}
	case 10:
		//line sql.y:150
		{
			yyVAL.Rower = isAsExpr(yyS[yypt-1].Rower, yyS[yypt-0].str)
		}
	case 11:
		//line sql.y:156
		{
			yyVAL.Rower = &whereRower{yyS[yypt-0].Wherer}
		}
	case 12:
		//line sql.y:160
		{
			yyVAL.Rower = yyS[yypt-0].Rower
		}
	case 13:
		//line sql.y:165
		{
			yyVAL.str = ""
		}
	case 14:
		//line sql.y:169
		{
			yyVAL.str = yyS[yypt-0].str
		}
	case 15:
		//line sql.y:173
		{
			yyVAL.str = yyS[yypt-0].str
		}
	case 16:
		//line sql.y:189
		{
			yyVAL.Tabler = isAsTable(yyS[yypt-1].Tabler, yyS[yypt-0].str)
		}
	case 17:
		//line sql.y:193
		{
			yyVAL.Tabler = yyS[yypt-1].Tabler
		}
	case 18:
		//line sql.y:197
		{
			yyVAL.Tabler = &joinQuery{yyS[yypt-4].Tabler, yyS[yypt-2].Tabler, yyS[yypt-0].Rowers}
		}
	case 19:
		//line sql.y:201
		{
			yyVAL.Tabler = makeValuesTable(yyS[yypt-3].ValueTuples, yyS[yypt-0].Rowers)
		}
	case 20:
		//line sql.y:206
		{
			yyVAL.str = ""
		}
	case 21:
		//line sql.y:210
		{
			yyVAL.str = yyS[yypt-0].str
		}
	case 22:
		//line sql.y:214
		{
			yyVAL.str = yyS[yypt-0].str
		}
	case 23:
		//line sql.y:220
		{
			yyVAL.str = "JOIN"
		}
	case 24:
		//line sql.y:227
		{
			yyVAL.Tabler = pickTable(yyS[yypt-0].str)
		}
	case 25:
		//line sql.y:231
		{
			yyVAL.Tabler = pickTable(yyS[yypt-0].str)
		}
	case 26:
		//line sql.y:235
		{
			yyVAL.Tabler = yyS[yypt-0].Tabler
		}
	case 27:
		//line sql.y:242
		{
			yyVAL.Wherer = nil
		}
	case 28:
		//line sql.y:246
		{
			yyVAL.Wherer = yyS[yypt-0].Wherer
		}
	case 29:
		yyVAL.Wherer = yyS[yypt-0].Wherer
	case 30:
		//line sql.y:253
		{
			yyVAL.Wherer = &andExpr{yyS[yypt-2].Wherer, yyS[yypt-0].Wherer}
		}
	case 31:
		//line sql.y:257
		{
			yyVAL.Wherer = &orExpr{yyS[yypt-2].Wherer, yyS[yypt-0].Wherer}
		}
	case 32:
		//line sql.y:261
		{
			yyVAL.Wherer = &notExpr{yyS[yypt-0].Wherer}
		}
	case 33:
		//line sql.y:265
		{
			yyVAL.Wherer = yyS[yypt-1].Wherer
		}
	case 34:
		//line sql.y:271
		{
			yyVAL.Wherer = &compExpr{yyS[yypt-2].Rower, yyS[yypt-0].Rower, yyS[yypt-1].str}
		}
	case 35:
		//line sql.y:275
		{
			yyVAL.Wherer = &isInExpr{yyS[yypt-2].Rower, yyS[yypt-0].Rowers}
		}
	case 36:
		//line sql.y:279
		{
			yyVAL.Wherer = &notExpr{&isInExpr{yyS[yypt-3].Rower, yyS[yypt-0].Rowers}}
		}
	case 37:
		//line sql.y:283
		{
			yyVAL.Wherer = &compExpr{yyS[yypt-2].Rower, yyS[yypt-0].Rower, "LIKE"}
		}
	case 38:
		//line sql.y:287
		{
			yyVAL.Wherer = &notExpr{&compExpr{yyS[yypt-3].Rower, yyS[yypt-0].Rower, "LIKE"}}
		}
	case 39:
		//line sql.y:291
		{
			yyVAL.Wherer = &rangeExpr{yyS[yypt-4].Rower, yyS[yypt-2].Rower, yyS[yypt-0].Rower}
		}
	case 40:
		//line sql.y:295
		{
			yyVAL.Wherer = &notExpr{&rangeExpr{yyS[yypt-5].Rower, yyS[yypt-2].Rower, yyS[yypt-0].Rower}}
		}
	case 41:
		//line sql.y:299
		{
			yyVAL.Wherer = &isNullExpr{yyS[yypt-2].Rower}
		}
	case 42:
		//line sql.y:303
		{
			yyVAL.Wherer = &notExpr{&isNullExpr{yyS[yypt-3].Rower}}
		}
	case 43:
		//line sql.y:313
		{
			yyVAL.str = "="
		}
	case 44:
		//line sql.y:317
		{
			yyVAL.str = "<"
		}
	case 45:
		//line sql.y:321
		{
			yyVAL.str = ">"
		}
	case 46:
		//line sql.y:325
		{
			yyVAL.str = "<="
		}
	case 47:
		//line sql.y:329
		{
			yyVAL.str = ">="
		}
	case 48:
		//line sql.y:333
		{
			yyVAL.str = "!="
		}
	case 49:
		//line sql.y:339
		{
			yyVAL.Rowers = yyS[yypt-1].Rowers
		}
	case 50:
		//line sql.y:353
		{
			yyVAL.Tabler = yyS[yypt-1].Tabler
		}
	case 51:
		//line sql.y:359
		{
			yyVAL.Values = []Value{yyS[yypt-0].Value}
		}
	case 52:
		//line sql.y:363
		{
			yyVAL.Values = append(yyS[yypt-2].Values, yyS[yypt-0].Value)
		}
	case 53:
		//line sql.y:370
		{
			yyVAL.Rower = yyS[yypt-0].Rower
		}
	case 54:
		//line sql.y:374
		{
			yyVAL.Rower = &valRow{"", yyS[yypt-0].Value}
		}
	case 55:
		//line sql.y:378
		{
			yyVAL.Rower = yyS[yypt-1].Rower
		}
	case 56:
		//line sql.y:390
		{
			yyVAL.Rower = &concatExpr{yyS[yypt-2].Rower, yyS[yypt-0].Rower}
		}
	case 57:
		//line sql.y:398
		{
			yyVAL.Rower = &mathExpr{yyS[yypt-2].Rower, yyS[yypt-0].Rower, "+"}
		}
	case 58:
		//line sql.y:402
		{
			yyVAL.Rower = &mathExpr{yyS[yypt-2].Rower, yyS[yypt-0].Rower, "-"}
		}
	case 59:
		//line sql.y:406
		{
			yyVAL.Rower = &mathExpr{yyS[yypt-2].Rower, yyS[yypt-0].Rower, "*"}
		}
	case 60:
		//line sql.y:410
		{
			yyVAL.Rower = &mathExpr{yyS[yypt-2].Rower, yyS[yypt-0].Rower, "/"}
		}
	case 61:
		//line sql.y:445
		{
			yyVAL.Rower = &funcExpr{yyS[yypt-3].str, yyS[yypt-1].Rowers}
		}
	case 62:
		//line sql.y:449
		{
			yyVAL.Rower = yyS[yypt-0].caseRower
		}
	case 63:
		//line sql.y:479
		{
			yyVAL.caseRower = &caseRower{yyS[yypt-2].whens, yyS[yypt-1].Rower}
		}
	case 64:
		//line sql.y:495
		{
			yyVAL.whens = []exPair{yyS[yypt-0].when}
		}
	case 65:
		//line sql.y:499
		{
			yyVAL.whens = append(yyS[yypt-1].whens, yyS[yypt-0].when)
		}
	case 66:
		//line sql.y:505
		{
			yyVAL.when = exPair{yyS[yypt-2].Wherer, yyS[yypt-0].Rower}
		}
	case 67:
		//line sql.y:510
		{
			yyVAL.Rower = nil
		}
	case 68:
		//line sql.y:514
		{
			yyVAL.Rower = yyS[yypt-0].Rower
		}
	case 69:
		//line sql.y:520
		{
			yyVAL.Rower = pickRow(yyS[yypt-0].str)
		}
	case 70:
		//line sql.y:524
		{
			yyVAL.Rower = pickRow(yyS[yypt-2].str) //&ColName{Qualifier: $1, Name: $3}
		}
	case 71:
		//line sql.y:530
		{
			yyVAL.Value = stringValue(yyS[yypt-0].str)
		}
	case 72:
		//line sql.y:534
		{
			yyVAL.Value = makeNumVal(yyS[yypt-0].str)
		}
	case 73:
		//line sql.y:538
		{
			yyVAL.Value = makeNumVal(yyS[yypt-0].str)
		}
	case 74:
		//line sql.y:542
		{
			yyVAL.Value = nullValue()
		}
	case 75:
		//line sql.y:548
		{
			yyVAL.orderList = nil
		}
	case 76:
		//line sql.y:552
		{
			yyVAL.orderList = yyS[yypt-0].orderList
		}
	case 77:
		//line sql.y:558
		{
			yyVAL.orderList = orderList{yyS[yypt-0].order}
		}
	case 78:
		//line sql.y:562
		{
			yyVAL.orderList = append(yyS[yypt-2].orderList, yyS[yypt-0].order)
		}
	case 79:
		//line sql.y:568
		{
			yyVAL.order = order{yyS[yypt-1].Rower, yyS[yypt-0].str}
		}
	case 80:
		//line sql.y:573
		{
			yyVAL.str = "ASC"
		}
	case 81:
		//line sql.y:577
		{
			yyVAL.str = "ASC"
		}
	case 82:
		//line sql.y:581
		{
			yyVAL.str = "DESC"
		}
	case 83:
		//line sql.y:587
		{
			yyVAL.Rowers = nil
		}
	case 84:
		//line sql.y:591
		{
			yyVAL.Rowers = yyS[yypt-1].Rowers
		}
	case 85:
		//line sql.y:597
		{
			yyVAL.Rowers = []Rower{yyS[yypt-0].Rower}
		}
	case 86:
		//line sql.y:601
		{
			yyVAL.Rowers = append(yyVAL.Rowers, yyS[yypt-0].Rower)
		}
	case 87:
		//line sql.y:608
		{
			yyVAL.ValueTuples = [][]Value{yyS[yypt-0].Values}
		}
	case 88:
		//line sql.y:612
		{
			yyVAL.ValueTuples = append(yyS[yypt-2].ValueTuples, yyS[yypt-0].Values)
		}
	case 89:
		//line sql.y:618
		{
			yyVAL.Values = []Value(yyS[yypt-1].Values)
		}
	case 90:
		//line sql.y:629
		{
			yyVAL.str = strings.ToLower(yyS[yypt-0].str)
		}
	}
	goto yystack /* stack new state and value */
}
