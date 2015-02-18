package sqlselect

import (
	"errors"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

const eof = -1

type ItemCode int

func (ic ItemCode) String() string {
	switch ic {
	case ItemError:
		return "Error"
	case ItemWhitespace:
		return "Whitespace"
	case ItemOpenParen:
		return "OpenParen"
	case ItemCloseParen:
		return "CloseParen"
	case ItemToken:
		return "Token"
	case ItemValue:
		return "Value"
	case ItemPunctuation:
		return "Punctuation"
	case ItemKeyword:
		return "Keyword"
	default:
		return "WTF"
	}
	panic("wtf")
}

const (
	ItemError ItemCode = iota
	ItemWhitespace
	ItemOpenParen
	ItemCloseParen
	ItemToken
	ItemValue
	ItemPunctuation
	ItemKeyword
)

type Item struct {
	Code ItemCode
	Item string
}

type lexer struct {
	name   string
	input  string
	parens int
	start  int
	pos    int
	width  int
	items  chan Item
}

type stateFunc func(lex *lexer) stateFunc

func (lex *lexer) run() {
	for stateFunc := lexText; stateFunc != nil; {
		stateFunc = stateFunc(lex)
	}
	close(lex.items)
}

func (lex *lexer) emit(c ItemCode) {
	lex.items <- Item{c, lex.input[lex.start:lex.pos]}
	lex.start = lex.pos
}

func (lex *lexer) emitKeyword() {
	s := lex.input[lex.start:lex.pos]
	isk, s := isKeyword(s)
	if isk {
		lex.items <- Item{ItemKeyword, s}
	} else {
		lex.items <- Item{ItemToken, s}
	}
	lex.start = lex.pos
}

// next returns the next rune in the input.
func (l *lexer) next() rune {
	if int(l.pos) >= len(l.input) {
		l.width = 0
		return eof
	}
	r := ' '
	r, l.width = utf8.DecodeRuneInString(l.input[l.pos:])
	l.pos += l.width
	return r
}

// peek returns but does not consume the next rune in the input.
func (l *lexer) peek() rune {
	r := l.next()
	l.backup()
	return r
}

// backup steps back one rune. Can only be called once per call of next.
func (l *lexer) backup() {
	l.pos -= l.width
}

func (l *lexer) ignore() {
	l.start = l.pos
}

func (l *lexer) accept(valid string) bool {
	if strings.IndexRune(valid, l.next()) > 0 {
		return true
	}
	l.backup()
	return false
}

func (l *lexer) acceptRun(valid string) {
	for strings.IndexRune(valid, l.next()) > 0 {
	}
	l.backup()
}

// errorf returns an error token and terminates the scan by passing
// back a nil pointer that will be the next state, terminating l.nextItem.
func (l *lexer) errorf(format string, args ...interface{}) stateFunc {
	l.items <- Item{ItemError, fmt.Sprintf(format, args...)}
	return nil
}
func lexText(lex *lexer) stateFunc {
	r := lex.peek()
	/*if strings.IndexRune("()\"'",r)>0 {
	    lex.ignore()
	    return lexText
	}*/
	//println(r)
	if r == eof {
		return nil
	} else if isWhitespace(r) {
		return lexWhitespace(lex)
	} else if unicode.IsLetter(r) {
		return lexToken(lex)
	} else if unicode.IsDigit(r) || r == '-' {
		return lexToken(lex)
	} else if r == '(' {
		return lexParen(lex)
	} else if r == ')' {
		return lexParen(lex)
	} else if r == '"' {
		return lexQuote(lex)
	} else if r == '\'' {
		return lexQuote(lex)
		/*} else if r=='!' {
		  return lexQuote(lex)*/
	} else if isPunctuation(r) {
		return lexPunctuation(lex)
	}
	return lex.errorf("not a space or an alpha {%s}", lex.input[lex.pos:])
}

func lexWhitespace(lex *lexer) stateFunc {

	for isWhitespace(lex.next()) {
	}
	lex.backup()

	lex.emit(ItemWhitespace)
	return lexText
}

func lexToken(lex *lexer) stateFunc {
	r := lex.next()
	if r == '-' || isAlpha(r) {
		for isAlpha(lex.next()) {
		}
	}
	lex.backup()

	s := lex.input[lex.start:lex.pos]
	if len(s) == 0 {
		lex.ignore()
		return lexText
	}
	if func() bool {
		for _, c := range s {
			if strings.IndexRune("0123456789.-", c) == -1 {
				return false
			}
		}
		return true
	}() {
		lex.emit(ItemValue)
	}
	lex.emitKeyword()
	return lexText
}

func lexParen(lex *lexer) stateFunc {
	r := lex.next()
	if r == '(' {
		lex.parens += 1
		lex.emit(ItemOpenParen)
	} else if r == ')' {
		lex.parens -= 1
		lex.emit(ItemCloseParen)
	} else {
		return lex.errorf("not a ( or )")
	}
	return lexText
}
func lexQuote(lex *lexer) stateFunc {
	r := lex.next()
	for g := lex.next(); g != r; g = lex.next() {
		if g == eof {
			lex.errorf("eof", lex.input[lex.start:])
		}
	}
	if r == '"' {
		lex.emit(ItemToken)
	} else {
		lex.emit(ItemValue)
	}
	return lexText
}

func Lex(input string) chan Item {
	items := make(chan Item)
	lex := &lexer{input: input, items: items}
	go lex.run()
	return items
}

func isWhitespace(r rune) bool {
	return r == ' ' || r == '\t' || r == '\n'
}

func isAlpha(r rune) bool {
	return r == '_' || unicode.IsLetter(r) || r == ':' || r == '.' || unicode.IsDigit(r)
}

func isPunctuation(r rune) bool {
	switch r {
	case '+', '-', '*', '!', ':', ';', '.', ',', '/', '\\', '=', '|', '>', '<':
		return true
	}
	return false
}

func lexPunctuation(lex *lexer) stateFunc {
	for isPunctuation(lex.next()) {
	}
	lex.backup()
	lex.emit(ItemPunctuation)
	return lexText
}

var keywords = map[string]bool{
	"select": true, "from": true, "where": true, "case": true,
	"when": true, "else": true, "end": true, "as": true, "in": true,
	"then": true, "join": true, "union": true, "is": true, "not": true,
	"null": true, "and": true, "or": true, "order": true, "by": true,
	"asc": true, "desc": true, "values": true, "using": true, "all": true,
	"between": true,
}

func isKeyword(s string) (bool, string) {
	if s == "" {
		return false, ""
	}
	ls := strings.ToLower(s)
	if s[0] == '"' {
		return false, ls
	}
	if _, ok := keywords[ls]; ok {
		return true, strings.ToUpper(s)
	}
	return false, ls
}

func LexList(input string) []Item {
	ll := make([]Item, 0, len(input)/5)

	for i := range Lex(input) {
		if i.Code == ItemWhitespace {
			continue
		}
		if i.Item == "" {
			continue
		}
		if i.Code == ItemPunctuation && strings.IndexRune(i.Item, '!') >= 0 {
			if len(i.Item) == 1 || i.Item == "!=" {
				ll = append(ll, i)
			} else {
				for _, c := range i.Item {
					ll = append(ll, Item{ItemPunctuation, string(c)})
				}
			}
		} else {
			ll = append(ll, i)
		}
	}

	ex := Item{ItemPunctuation, "!"}
	pp := make([]Item, 0, len(ll))
	for i := 0; i < len(ll); i++ {
		if i < (len(ll)-2) && ll[i] == ex && ll[i+1].Code == ItemToken && ll[i+2] == ex {
			pp = append(pp, Item{ItemToken, "!" + ll[i+1].Item + "!"})
			i += 2
		} else {
			pp = append(pp, ll[i])
		}
	}

	qq := make([]Item, 0, len(pp))

	dc := Item{ItemPunctuation, "::"}

	for i := 0; i < len(pp); i++ {
		if i < (len(pp)-2) && pp[i].Code == ItemValue && pp[i+1] == dc && pp[i+2].Code == ItemToken {
			ni := Item{ItemValue, pp[i].Item + "::" + pp[i+2].Item}
			qq = append(qq, ni)
			i += 2
		} else {
			qq = append(qq, pp[i])

		}
	}

	rr := append(make([]Item, 0, len(qq)), qq[0])

	for i := 1; i < len(qq); i++ {
		if qq[i].Code == ItemValue && qq[i].Item[0] == '-' && qq[i-1].Code != ItemPunctuation {
			rr = append(rr, Item{ItemPunctuation, "-"})
			rr = append(rr, Item{ItemValue, qq[i].Item[1:]})
		} else {
			rr = append(rr, qq[i])
		}
	}

	return rr
}

type LexResult struct {
	items  []Item
	pl     int
	err    bool
	result Tabler
}

func (l *LexResult) Error(s string) {
	l.err = true
	fmt.Printf("prob @ line %d %s\n", l.pl, s)
	si := l.pl - 5
	if si < 0 {
		si = 0
	}
	ti := l.pl + 6
	if ti > len(l.items) {
		ti = len(l.items)
	}
	for i := si; i < ti; i++ {
		fmt.Printf("[%-4d]: %s\n", i, l.items[i])
	}

}

func toLexInt(item string) int {
	switch item {
	case "SELECT":
		return SELECT
	case "FROM":
		return FROM
	case "WHERE":
		return WHERE
	case "ORDER":
		return ORDER
	case "BY":
		return BY
	case "CASE":
		return CASE
	case "WHEN":
		return WHEN
	case "THEN":
		return THEN
	case "ELSE":
		return ELSE
	case "END":
		return END
	case "IN":
		return IN
	case "IS":
		return IS
	case "NOT":
		return NOT
	case "NULL":
		return NULL
	case "AS":
		return AS
	case "AND":
		return AND
	case "OR":
		return OR
	case "UNION":
		return UNION
	case "JOIN":
		return JOIN
	case "USING":
		return USING
	case "ASC":
		return ASC
	case "DESC":
		return DESC
	case "VALUES":
		return VALUES
	case "ALL":
		return ALL
	case "BETWEEN":
		return BETWEEN
		//case "COALESCE": return COALESCE
	}
	return LEX_ERROR
}

func (l *LexResult) Lex(lval *yySymType) int {
	l.pl++
	if l.pl == len(l.items) {
		return 0
	}
	*lval = yySymType{str: l.items[l.pl].Item}

	c := l.items[l.pl].Code
	switch c {
	case ItemToken:
		return ID
	case ItemValue:
		for _, c := range lval.str {
			if strings.IndexRune("0123456789.-", c) < 0 {
				return STRING
			}
		}
		return NUMBER
	case ItemPunctuation:
		switch lval.str {
		case ",":
			return int(lval.str[0])
		case "*":
			return ST
		case "/":
			return DV
		case "+":
			return PL
		case "-":
			return MN
		case "||":
			return BR
		case "=":
			return EQ
		case "!=":
			return NE
		case ">":
			return GT
		case "<":
			return LT
		case ">=":
			return GE
		case "<=":
			return LE
		case "<>":
			return NE

		}
		return STRING
	case ItemOpenParen:
		return int('(')
	case ItemCloseParen:
		return int(')')
	case ItemKeyword:
		return toLexInt(l.items[l.pl].Item)
	}
	fmt.Printf("?? %d: %v %d\n", l.pl, lval.str, c)

	return LEX_ERROR
}

func Parse(input string) (Tabler, error) {
	l := &LexResult{LexList(input), -1, false, nil}
	//fmt.Printf("%v %d\n",l.items, l.pl)
	e := yyParse(l)
	if e == 0 && !l.err {
		return l.result, nil
	}
	return nil, errors.New("Compilation halted due to lex and parse errors")
}
