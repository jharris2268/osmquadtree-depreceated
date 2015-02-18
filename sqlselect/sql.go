//line sql.y:6
package sqlselect

import __yyfmt__ "fmt"

//line sql.y:6
import "strings"

//line sql.y:11
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
	-1, 139,
	34, 5,
	55, 5,
	-2, 48,
}

const yyNprod = 89
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 251

var yyAct = []int{

	54, 18, 16, 142, 12, 144, 17, 124, 75, 99,
	102, 8, 32, 2, 36, 35, 56, 114, 26, 27,
	42, 43, 44, 45, 46, 60, 61, 106, 5, 87,
	161, 12, 88, 148, 5, 70, 136, 11, 73, 98,
	64, 79, 63, 82, 83, 84, 85, 86, 160, 139,
	30, 149, 53, 55, 135, 126, 122, 24, 12, 58,
	90, 20, 21, 22, 23, 30, 5, 35, 129, 89,
	15, 150, 80, 71, 72, 107, 61, 12, 93, 110,
	104, 134, 14, 113, 98, 92, 109, 62, 108, 30,
	25, 36, 35, 5, 115, 38, 41, 81, 40, 94,
	45, 46, 67, 116, 36, 35, 20, 119, 50, 51,
	52, 66, 131, 47, 49, 48, 145, 132, 97, 127,
	43, 44, 45, 46, 39, 42, 43, 44, 45, 46,
	9, 140, 138, 3, 143, 120, 130, 88, 42, 43,
	44, 45, 46, 125, 147, 146, 76, 156, 155, 98,
	57, 143, 157, 101, 159, 158, 38, 41, 4, 40,
	128, 69, 100, 156, 162, 121, 34, 3, 112, 50,
	51, 52, 28, 133, 47, 49, 48, 111, 3, 42,
	43, 44, 45, 46, 118, 39, 42, 43, 44, 45,
	46, 24, 4, 103, 67, 20, 21, 22, 23, 77,
	33, 24, 78, 105, 15, 20, 21, 22, 23, 34,
	152, 153, 24, 29, 74, 137, 14, 21, 22, 23,
	7, 154, 151, 141, 25, 42, 43, 44, 45, 46,
	117, 91, 59, 19, 25, 42, 43, 44, 45, 46,
	68, 123, 37, 13, 95, 65, 96, 10, 31, 6,
	1,
}
var yyPact = []int{

	128, -1000, 59, 174, 128, 162, 207, -1000, 52, -1000,
	188, 64, 143, -1000, 40, 40, -1000, -1000, 120, -1000,
	11, -1000, -1000, -1000, -1000, -25, 32, -1000, 128, 81,
	40, -1000, -1000, 145, -1000, 40, 40, 184, 116, 186,
	184, 55, 184, 184, 184, 184, 184, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, 143, -26, 82, 40, 145, 25,
	-1000, 40, -1000, 59, 111, 141, 173, -21, -1000, -1000,
	-1000, -1000, 26, 182, 184, -1000, 40, 116, 184, 136,
	-1000, 151, 76, 54, 54, -1000, -1000, -1000, -1000, 28,
	-1000, -37, -1000, 184, 51, 176, 81, 40, -1000, -1000,
	-1000, 144, 1, 113, 0, 173, 139, -23, 13, -1000,
	95, 184, -1000, -1000, -1000, 182, 184, -1000, 164, 46,
	64, -1000, -1000, -1, -1000, 195, -1000, -6, -1000, -1000,
	184, 182, 182, 184, 86, 141, 113, -4, -1000, -1000,
	182, 34, -1000, 192, -1000, 85, 86, -1000, 195, -1000,
	184, -1000, -1000, -1000, -7, -1000, -1000, -1000, -1000, -1000,
	-1000, 85, -1000,
}
var yyPgo = []int{

	0, 250, 13, 249, 11, 130, 248, 9, 247, 10,
	246, 245, 244, 37, 243, 242, 6, 0, 8, 241,
	7, 240, 2, 233, 232, 25, 231, 230, 223, 222,
	5, 221, 1, 3, 215,
}
var yyR1 = []int{

	0, 1, 2, 2, 2, 2, 3, 3, 4, 4,
	5, 8, 8, 6, 6, 6, 9, 9, 9, 9,
	7, 7, 7, 10, 11, 11, 11, 12, 12, 13,
	13, 13, 13, 13, 14, 14, 14, 14, 14, 14,
	14, 15, 15, 15, 15, 15, 15, 18, 21, 34,
	34, 17, 17, 17, 17, 17, 17, 17, 17, 17,
	17, 23, 24, 24, 25, 26, 26, 22, 22, 16,
	16, 16, 16, 27, 27, 28, 28, 33, 29, 29,
	29, 30, 30, 31, 31, 19, 19, 20, 32,
}
var yyR2 = []int{

	0, 1, 6, 3, 4, 3, 1, 1, 1, 3,
	2, 1, 1, 0, 1, 2, 2, 3, 5, 6,
	0, 1, 2, 1, 1, 3, 1, 0, 2, 1,
	3, 3, 2, 3, 3, 3, 4, 5, 6, 3,
	4, 1, 1, 1, 1, 1, 1, 3, 3, 1,
	3, 1, 1, 3, 3, 3, 3, 3, 3, 4,
	1, 4, 1, 2, 4, 0, 2, 1, 3, 1,
	1, 1, 1, 0, 3, 1, 3, 2, 0, 1,
	1, 0, 3, 1, 3, 1, 3, 3, 1,
}
var yyChk = []int{

	-1000, -1, -2, 5, 30, 34, -3, 46, -4, -5,
	-8, -13, -17, -14, 42, 30, -22, -16, -32, -23,
	21, 22, 23, 24, 17, 50, -2, -2, 10, 6,
	37, -6, -32, 12, 21, 41, 40, -15, 13, 42,
	16, 14, 43, 44, 45, 46, 47, 31, 33, 32,
	26, 27, 28, -13, -17, -13, -17, 30, 48, -24,
	-25, 51, 55, -2, -9, -11, 30, 21, -21, -5,
	-32, -13, -13, -17, 30, -18, 30, 13, 16, -17,
	17, 42, -17, -17, -17, -17, -17, 55, 55, -4,
	-32, -26, -25, 53, -13, -12, -10, 7, 38, -7,
	21, 12, -9, 20, -2, 30, 48, -17, -4, -18,
	-17, 41, 17, 55, 54, -17, 52, -27, 8, -9,
	-13, 21, 55, -19, -20, 30, 55, -2, 21, 55,
	41, -17, -17, 9, 35, 55, 37, -34, -16, 55,
	-17, -28, -33, -17, -30, 30, -7, -20, 37, 55,
	37, -29, 18, 19, -31, -22, -32, -30, -16, -33,
	55, 37, -22,
}
var yyDef = []int{

	0, -2, 1, 0, 0, 0, 0, 6, 7, 8,
	13, 11, 12, 29, 0, 0, 51, 52, 67, 60,
	88, 69, 70, 71, 72, 0, 0, 3, 0, 0,
	0, 10, 14, 0, 88, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 41, 42, 43,
	44, 45, 46, 32, 0, 0, 0, 0, 0, 65,
	62, 0, 5, 4, 27, 20, 0, 24, 26, 9,
	15, 30, 31, 34, 0, 35, 0, 0, 0, 0,
	39, 0, 54, 55, 56, 57, 58, 33, 53, 0,
	68, 0, 63, 0, 0, 73, 0, 0, 23, 16,
	21, 0, 0, 0, 0, 0, 0, 0, 0, 36,
	0, 0, 40, 59, 61, 66, 0, 2, 0, 0,
	28, 22, 17, 0, 85, 0, 48, 0, 25, 47,
	0, 37, 64, 0, 81, 20, 0, 0, 49, -2,
	38, 74, 75, 78, 18, 0, 81, 86, 0, 87,
	0, 77, 79, 80, 0, 83, 67, 19, 50, 76,
	82, 0, 84,
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
		//line sql.y:97
		{
			yylex.(*LexResult).result = yyS[yypt-0].Tabler
			return 0
		}
	case 2:
		//line sql.y:106
		{
			yyVAL.Tabler = makeSimpleSelect(yyS[yypt-2].Tabler, yyS[yypt-4].Rowers, yyS[yypt-1].Wherer, yyS[yypt-0].orderList)
		}
	case 3:
		//line sql.y:110
		{
			yyVAL.Tabler = &unionQuery{yyS[yypt-2].Tabler, yyS[yypt-0].Tabler, "", ""}
		}
	case 4:
		//line sql.y:114
		{
			yyVAL.Tabler = &unionQuery{yyS[yypt-3].Tabler, yyS[yypt-0].Tabler, "", ""}
		}
	case 5:
		//line sql.y:118
		{
			yyVAL.Tabler = yyS[yypt-1].Tabler
		}
	case 6:
		//line sql.y:125
		{
			yyVAL.Rowers = nil
		}
	case 7:
		//line sql.y:130
		{
			yyVAL.Rowers = yyS[yypt-0].Rowers
		}
	case 8:
		//line sql.y:137
		{
			yyVAL.Rowers = rowerList{yyS[yypt-0].Rower}
		}
	case 9:
		//line sql.y:141
		{
			yyVAL.Rowers = append(yyVAL.Rowers, yyS[yypt-0].Rower)
		}
	case 10:
		//line sql.y:147
		{
			yyVAL.Rower = isAsExpr(yyS[yypt-1].Rower, yyS[yypt-0].str)
		}
	case 11:
		//line sql.y:153
		{
			yyVAL.Rower = &whereRower{yyS[yypt-0].Wherer}
		}
	case 12:
		//line sql.y:157
		{
			yyVAL.Rower = yyS[yypt-0].Rower
		}
	case 13:
		//line sql.y:162
		{
			yyVAL.str = ""
		}
	case 14:
		//line sql.y:166
		{
			yyVAL.str = yyS[yypt-0].str
		}
	case 15:
		//line sql.y:170
		{
			yyVAL.str = yyS[yypt-0].str
		}
	case 16:
		//line sql.y:186
		{
			yyVAL.Tabler = isAsTable(yyS[yypt-1].Tabler, yyS[yypt-0].str)
		}
	case 17:
		//line sql.y:190
		{
			yyVAL.Tabler = yyS[yypt-1].Tabler
		}
	case 18:
		//line sql.y:194
		{
			yyVAL.Tabler = &joinQuery{yyS[yypt-4].Tabler, yyS[yypt-2].Tabler, yyS[yypt-0].Rowers}
		}
	case 19:
		//line sql.y:198
		{
			yyVAL.Tabler = makeValuesTable(yyS[yypt-3].ValueTuples, yyS[yypt-0].Rowers)
		}
	case 20:
		//line sql.y:203
		{
			yyVAL.str = ""
		}
	case 21:
		//line sql.y:207
		{
			yyVAL.str = yyS[yypt-0].str
		}
	case 22:
		//line sql.y:211
		{
			yyVAL.str = yyS[yypt-0].str
		}
	case 23:
		//line sql.y:217
		{
			yyVAL.str = "JOIN"
		}
	case 24:
		//line sql.y:224
		{
			yyVAL.Tabler = pickTable(yyS[yypt-0].str)
		}
	case 25:
		//line sql.y:228
		{
			yyVAL.Tabler = pickTable(yyS[yypt-0].str)
		}
	case 26:
		//line sql.y:232
		{
			yyVAL.Tabler = yyS[yypt-0].Tabler
		}
	case 27:
		//line sql.y:239
		{
			yyVAL.Wherer = nil
		}
	case 28:
		//line sql.y:243
		{
			yyVAL.Wherer = yyS[yypt-0].Wherer
		}
	case 29:
		yyVAL.Wherer = yyS[yypt-0].Wherer
	case 30:
		//line sql.y:250
		{
			yyVAL.Wherer = &andExpr{yyS[yypt-2].Wherer, yyS[yypt-0].Wherer}
		}
	case 31:
		//line sql.y:254
		{
			yyVAL.Wherer = &orExpr{yyS[yypt-2].Wherer, yyS[yypt-0].Wherer}
		}
	case 32:
		//line sql.y:258
		{
			yyVAL.Wherer = &notExpr{yyS[yypt-0].Wherer}
		}
	case 33:
		//line sql.y:262
		{
			yyVAL.Wherer = yyS[yypt-1].Wherer
		}
	case 34:
		//line sql.y:268
		{
			yyVAL.Wherer = &compExpr{yyS[yypt-2].Rower, yyS[yypt-0].Rower, yyS[yypt-1].str}
		}
	case 35:
		//line sql.y:272
		{
			yyVAL.Wherer = &isInExpr{yyS[yypt-2].Rower, yyS[yypt-0].Rowers}
		}
	case 36:
		//line sql.y:276
		{
			yyVAL.Wherer = &notExpr{&isInExpr{yyS[yypt-3].Rower, yyS[yypt-0].Rowers}}
		}
	case 37:
		//line sql.y:288
		{
			yyVAL.Wherer = &rangeExpr{yyS[yypt-4].Rower, yyS[yypt-2].Rower, yyS[yypt-0].Rower}
		}
	case 38:
		//line sql.y:292
		{
			yyVAL.Wherer = &notExpr{&rangeExpr{yyS[yypt-5].Rower, yyS[yypt-2].Rower, yyS[yypt-0].Rower}}
		}
	case 39:
		//line sql.y:296
		{
			yyVAL.Wherer = &isNullExpr{yyS[yypt-2].Rower}
		}
	case 40:
		//line sql.y:300
		{
			yyVAL.Wherer = &notExpr{&isNullExpr{yyS[yypt-3].Rower}}
		}
	case 41:
		//line sql.y:310
		{
			yyVAL.str = "="
		}
	case 42:
		//line sql.y:314
		{
			yyVAL.str = "<"
		}
	case 43:
		//line sql.y:318
		{
			yyVAL.str = ">"
		}
	case 44:
		//line sql.y:322
		{
			yyVAL.str = "<="
		}
	case 45:
		//line sql.y:326
		{
			yyVAL.str = ">="
		}
	case 46:
		//line sql.y:330
		{
			yyVAL.str = "!="
		}
	case 47:
		//line sql.y:336
		{
			yyVAL.Rowers = yyS[yypt-1].Rowers
		}
	case 48:
		//line sql.y:350
		{
			yyVAL.Tabler = yyS[yypt-1].Tabler
		}
	case 49:
		//line sql.y:356
		{
			yyVAL.Values = []Value{yyS[yypt-0].Value}
		}
	case 50:
		//line sql.y:360
		{
			yyVAL.Values = append(yyS[yypt-2].Values, yyS[yypt-0].Value)
		}
	case 51:
		//line sql.y:367
		{
			yyVAL.Rower = yyS[yypt-0].Rower
		}
	case 52:
		//line sql.y:371
		{
			yyVAL.Rower = &valRow{"", yyS[yypt-0].Value}
		}
	case 53:
		//line sql.y:375
		{
			yyVAL.Rower = yyS[yypt-1].Rower
		}
	case 54:
		//line sql.y:387
		{
			yyVAL.Rower = &concatExpr{yyS[yypt-2].Rower, yyS[yypt-0].Rower}
		}
	case 55:
		//line sql.y:395
		{
			yyVAL.Rower = &mathExpr{yyS[yypt-2].Rower, yyS[yypt-0].Rower, "+"}
		}
	case 56:
		//line sql.y:399
		{
			yyVAL.Rower = &mathExpr{yyS[yypt-2].Rower, yyS[yypt-0].Rower, "-"}
		}
	case 57:
		//line sql.y:403
		{
			yyVAL.Rower = &mathExpr{yyS[yypt-2].Rower, yyS[yypt-0].Rower, "*"}
		}
	case 58:
		//line sql.y:407
		{
			yyVAL.Rower = &mathExpr{yyS[yypt-2].Rower, yyS[yypt-0].Rower, "/"}
		}
	case 59:
		//line sql.y:442
		{
			yyVAL.Rower = &funcExpr{yyS[yypt-3].str, yyS[yypt-1].Rowers}
		}
	case 60:
		//line sql.y:446
		{
			yyVAL.Rower = yyS[yypt-0].caseRower
		}
	case 61:
		//line sql.y:476
		{
			yyVAL.caseRower = &caseRower{yyS[yypt-2].whens, yyS[yypt-1].Rower}
		}
	case 62:
		//line sql.y:492
		{
			yyVAL.whens = []exPair{yyS[yypt-0].when}
		}
	case 63:
		//line sql.y:496
		{
			yyVAL.whens = append(yyS[yypt-1].whens, yyS[yypt-0].when)
		}
	case 64:
		//line sql.y:502
		{
			yyVAL.when = exPair{yyS[yypt-2].Wherer, yyS[yypt-0].Rower}
		}
	case 65:
		//line sql.y:507
		{
			yyVAL.Rower = nil
		}
	case 66:
		//line sql.y:511
		{
			yyVAL.Rower = yyS[yypt-0].Rower
		}
	case 67:
		//line sql.y:517
		{
			yyVAL.Rower = pickRow(yyS[yypt-0].str)
		}
	case 68:
		//line sql.y:521
		{
			yyVAL.Rower = pickRow(yyS[yypt-2].str) //&ColName{Qualifier: $1, Name: $3}
		}
	case 69:
		//line sql.y:527
		{
			yyVAL.Value = stringValue(yyS[yypt-0].str)
		}
	case 70:
		//line sql.y:531
		{
			yyVAL.Value = makeNumVal(yyS[yypt-0].str)
		}
	case 71:
		//line sql.y:535
		{
			yyVAL.Value = makeNumVal(yyS[yypt-0].str)
		}
	case 72:
		//line sql.y:539
		{
			yyVAL.Value = nullValue()
		}
	case 73:
		//line sql.y:545
		{
			yyVAL.orderList = nil
		}
	case 74:
		//line sql.y:549
		{
			yyVAL.orderList = yyS[yypt-0].orderList
		}
	case 75:
		//line sql.y:555
		{
			yyVAL.orderList = orderList{yyS[yypt-0].order}
		}
	case 76:
		//line sql.y:559
		{
			yyVAL.orderList = append(yyS[yypt-2].orderList, yyS[yypt-0].order)
		}
	case 77:
		//line sql.y:565
		{
			yyVAL.order = order{yyS[yypt-1].Rower, yyS[yypt-0].str}
		}
	case 78:
		//line sql.y:570
		{
			yyVAL.str = "ASC"
		}
	case 79:
		//line sql.y:574
		{
			yyVAL.str = "ASC"
		}
	case 80:
		//line sql.y:578
		{
			yyVAL.str = "DESC"
		}
	case 81:
		//line sql.y:584
		{
			yyVAL.Rowers = nil
		}
	case 82:
		//line sql.y:588
		{
			yyVAL.Rowers = yyS[yypt-1].Rowers
		}
	case 83:
		//line sql.y:594
		{
			yyVAL.Rowers = []Rower{yyS[yypt-0].Rower}
		}
	case 84:
		//line sql.y:598
		{
			yyVAL.Rowers = append(yyVAL.Rowers, yyS[yypt-0].Rower)
		}
	case 85:
		//line sql.y:605
		{
			yyVAL.ValueTuples = [][]Value{yyS[yypt-0].Values}
		}
	case 86:
		//line sql.y:609
		{
			yyVAL.ValueTuples = append(yyS[yypt-2].ValueTuples, yyS[yypt-0].Values)
		}
	case 87:
		//line sql.y:615
		{
			yyVAL.Values = []Value(yyS[yypt-1].Values)
		}
	case 88:
		//line sql.y:626
		{
			yyVAL.str = strings.ToLower(yyS[yypt-0].str)
		}
	}
	goto yystack /* stack new state and value */
}
