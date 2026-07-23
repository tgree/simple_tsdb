import argparse
import math
import sys
from enum import IntEnum


WORD_LEAD_CHARS = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_'
WORD_CHARS = WORD_LEAD_CHARS + '0123456789'


def func_abs(stack):
    pass


def func_sin(stack):
    assert len(stack) >= 1
    v = stack.pop()
    assert isinstance(v, (int, float))
    return math.sin(v)


def func_cos(stack):
    pass


def func_log10(stack):
    pass


def func_log2(stack):
    pass


def func_ln(stack):
    pass


def func_exp(stack):
    pass


def func_pow(stack):
    # lhs , rhs
    assert len(stack) >= 3
    rhs = stack.pop()
    assert stack.pop() == ','
    lhs = stack.pop()
    assert isinstance(rhs, (int, float))
    assert isinstance(lhs, (int, float))
    return pow(lhs, rhs)


def func_min(stack):
    pass


def func_max(stack):
    pass


FUNCTIONS = {
    'abs'   : func_abs,
    'sin'   : func_sin,
    'cos'   : func_cos,
    'log10' : func_log10,
    'log2'  : func_log2,
    'ln'    : func_ln,
    'exp'   : func_exp,
    'pow'   : func_pow,
    'min'   : func_min,
    'max'   : func_max,
}

VARIABLES = {
    'var1' : 123.45,
    'a'    : 111e-1,
    'z'    : 42,
}

PRECEDENCE = {
    'NEG' : 4,
    '*'   : 3,
    '/'   : 3,
    '+'   : 2,
    '-'   : 2,
}

ASSOCIATIVITY = {
    'NEG' : 'R',
    '*'   : 'L',
    '/'   : 'L',
    '+'   : 'L',
    '-'   : 'L',
}


class TokenType:
    OPEN_PAREN  = 1
    CLOSE_PAREN = 2
    OPERATOR    = 3
    COMMA       = 4
    NUMBER      = 5
    STRING      = 6
    FUNCTION    = 7
    VARIABLE    = 8


class Token:
    def __init__(self, token_type, data=None):
        self.token_type = token_type
        self.data = data

    def __repr__(self):
        if self.token_type == TokenType.OPEN_PAREN:
            return "'('"
        if self.token_type == TokenType.CLOSE_PAREN:
            return "')'"
        if self.token_type == TokenType.OPERATOR:
            return "'%s'" % self.data
        if self.token_type == TokenType.COMMA:
            return "','"
        if self.token_type == TokenType.NUMBER:
            return '%s' % self.data
        if self.token_type == TokenType.STRING:
            return "STRING('%s')" % self.data
        if self.token_type == TokenType.FUNCTION:
            return "FUNCTION('%s')" % self.data
        if self.token_type == TokenType.VARIABLE:
            return "VARIABLE('%s')" % self.data
        raise Exception('Unrecognized token type!')


class CharStream:
    def __init__(self, string):
        self.string = string
        self.pos = 0

    def pop_number(self):
        # Get the integer part.
        ipart = 0
        ndigits = 0
        while self.pos < len(self.string):
            c = self.string[self.pos]
            if c in '.e':
                break
            if c in '+-*/),' or c.isspace():
                if ndigits == 0:
                    raise Exception('No digits for number.')
                return Token(TokenType.NUMBER, ipart)
            if c not in '0123456789':
                raise Exception('Invalid number.')

            self.pos += 1
            ipart = 10 * ipart + int(c)
            ndigits += 1
        if self.pos >= len(self.string):
            if ndigits == 0:
                raise Exception('No digits for number.')
            return Token(TokenType.NUMBER, ipart)

        # Get the fractional part and divisor.
        fpart = 0
        fdiv  = 1
        if c == '.':
            self.pos += 1
            while self.pos < len(self.string):
                c = self.string[self.pos]
                if c == 'e':
                    break
                if c in '+-*/), ':
                    if ndigits == 0:
                        raise Exception('No digits for number.')
                    return Token(TokenType.NUMBER, ipart + fpart / fdiv)
                if c not in '0123456789':
                    raise Exception('Invalid number.')

                self.pos += 1
                fpart = 10 * fpart + int(c)
                fdiv *= 10
                ndigits += 1
        if ndigits == 0:
            raise Exception('No digits for number.')
        if self.pos >= len(self.string):
            return Token(TokenType.NUMBER, ipart + fpart / fdiv)

        # Get the exponent.
        assert c == 'e'
        exponent = 0
        ndigits = 0
        self.pos += 1
        if self.pos >= len(self.string) or self.string[self.pos].isspace():
            raise Exception('Invalid exponent.')
        multiplier = 1
        c = self.string[self.pos]
        if c in '+-':
            self.pos += 1
            if c == '-':
                multiplier = -1
            if self.pos >= len(self.string) or self.string[self.pos].isspace():
                raise Exception('Invalid exponent.')
        while self.pos < len(self.string):
            c = self.string[self.pos]
            if c in '+-*/(),' or c.isspace():
                break
            if c not in '0123456789':
                raise Exception('Invalid exponent.')

            self.pos += 1
            ndigits += 1
            exponent = 10 * exponent + int(c)
        if ndigits == 0:
            raise Exception('Invalid exponent.')
        return Token(TokenType.NUMBER,
                     (ipart + fpart / fdiv) * pow(10, multiplier * exponent))

    def pop_string(self):
        p0 = self.pos
        assert p0 < len(self.string)
        assert self.string[p0] in WORD_LEAD_CHARS

        p = p0 + 1
        while p < len(self.string) and self.string[p] in WORD_CHARS:
            p += 1

        self.pos = p
        return Token(TokenType.STRING, self.string[p0:p])

    def pop_token(self):
        # Advance to the start of the next token.
        p = self.pos
        while p < len(self.string) and self.string[p].isspace():
            p += 1
        self.pos = p

        # If we are at the end of the string, no tokens are left.
        if self.pos >= len(self.string):
            return None

        # Handle single-character tokens.
        c = self.string[self.pos]
        if c == '(':
            self.pos += 1
            return Token(TokenType.OPEN_PAREN)
        if c == ')':
            self.pos += 1
            return Token(TokenType.CLOSE_PAREN)
        if c == ',':
            self.pos += 1
            return Token(TokenType.COMMA)
        if c in '+-/*':
            self.pos += 1
            return Token(TokenType.OPERATOR, c)

        # Check for a number token.
        if c in '0123456789.':
            return self.pop_number()

        # Check for a string token.
        if c in WORD_LEAD_CHARS:
            return self.pop_string()

        raise Exception('Illegal token at position %u.' % self.pos)


def process_tokens_1(tokens):
    '''
    First pass oveer the token array.  Convert STRING tokens to FUNCTION or
    VARIABLE.  Convert OPERATOR '-' to 'NEG' where appropriate.  Discard
    OPERATOR '+' where it is just a sign character.  Discard 'NEG' pairs.
    '''
    new_tokens = []
    for token in tokens:
        prev_token = new_tokens[-1] if new_tokens else None
        tt = token.token_type

        # If we have a previous token, check what we can have now.
        if prev_token is not None:
            ptt = prev_token.token_type

            # If the last token was a close-paren, or a number, or a variable,
            # then we can either have another close-paren, a math operator or
            # a comma.
            if ptt in (TokenType.CLOSE_PAREN,
                       TokenType.NUMBER,
                       TokenType.VARIABLE):
                if tt in (TokenType.CLOSE_PAREN,
                          TokenType.OPERATOR,
                          TokenType.COMMA):
                    new_tokens.append(token)
                    continue
                raise Exception('Unexpected token: %s' % token)

            # If the last token was a string and the next token is an open-
            # paren, then we have a function call, otherwise the last token was
            # a variable name.
            if ptt == TokenType.STRING:
                if tt == TokenType.OPEN_PAREN:
                    new_tokens[-1].token_type = TokenType.FUNCTION
                else:
                    new_tokens[-1].token_type = TokenType.VARIABLE
                new_tokens.append(token)
                continue

            # Handle a previous negate if we can.
            if ptt == TokenType.OPERATOR and prev_token.data == 'NEG':
                if tt == TokenType.OPERATOR:
                    # If the current one is a negate, discard both.
                    if token.data == '-':
                        new_tokens.pop()
                        continue
                    elif token.data == '+':
                        continue
                if tt == TokenType.NUMBER:
                    new_tokens.pop()
                    new_tokens.append(Token(TokenType.NUMBER, -token.data))
                    continue

        # There are no tokens, or the last token was an open-paren, a math
        # operator or a comma.  We can either have another open-paren, a
        # number (possibly preceded by a sign character), or a string.
        # Handle open-paren easily.
        if tt == TokenType.OPEN_PAREN:
            new_tokens.append(token)
            continue

        # Handle a sign character.
        if tt == TokenType.OPERATOR and token.data in '+-':
            if token.data == '+':
                continue
            token.data = 'NEG'
            new_tokens.append(token)
            continue

        # Handle a string or a number.
        new_tokens.append(token)

    # If the final token was a string, it must be a variable.
    if new_tokens and new_tokens[-1].token_type == TokenType.STRING:
        new_tokens[-1].token_type = TokenType.VARIABLE

    return new_tokens


def process_tokens_2(tokens):
    '''
    Ensure that VARIABLE and FUNCTION tokens map to real names.
    '''
    for token in tokens:
        if token.token_type == TokenType.FUNCTION:
            if token.data not in FUNCTIONS:
                raise Exception('No such function %s.' % token.data)
        elif token.token_type == TokenType.VARIABLE:
            if token.data not in VARIABLES:
                raise Exception('No such variable %s.' % token.data)

    return tokens


def shunting_yard_tokens(tokens):
    '''
    Runs the shunting-yard algorithm on the list of tokens to produce an output
    queue for evaluation.
    '''
    output = []
    operator_stack = []
    for token in tokens:
        tt = token.token_type
        if tt in (TokenType.NUMBER,
                  TokenType.VARIABLE):
            output.append(token)
        elif tt in (TokenType.FUNCTION,
                    TokenType.OPEN_PAREN):
            operator_stack.append(token)
        elif tt == TokenType.OPERATOR:
            while operator_stack:
                opt = operator_stack[-1]
                if opt.token_type == TokenType.OPEN_PAREN:
                    break
                assert opt.token_type == TokenType.OPERATOR

                prec_opt = PRECEDENCE[opt.data]
                prec = PRECEDENCE[token.data]
                assc = ASSOCIATIVITY[token.data]
                if prec_opt > prec or (prec_opt == prec and assc == 'L'):
                    output.append(opt)
                    operator_stack.pop()
                    continue

                break
            operator_stack.append(token)
        elif tt == TokenType.COMMA:
            while operator_stack:
                opt = operator_stack[-1]
                if opt.token_type == TokenType.OPEN_PAREN:
                    break
                assert opt.token_type == TokenType.OPERATOR

                output.append(opt)
                operator_stack.pop()
            output.append(token)
        elif tt == TokenType.CLOSE_PAREN:
            while operator_stack:
                opt = operator_stack[-1]
                if opt.token_type == TokenType.OPEN_PAREN:
                    break

                output.append(opt)
                operator_stack.pop()

            assert operator_stack
            assert operator_stack[-1].token_type == TokenType.OPEN_PAREN
            operator_stack.pop()

            if operator_stack:
                if operator_stack[-1].token_type == TokenType.FUNCTION:
                    output.append(operator_stack.pop())

    while operator_stack:
        assert operator_stack[-1].token_type != TokenType.OPEN_PAREN
        output.append(operator_stack.pop())

    return output


def evaluate_tokens(tokens):
    stack = []
    for token in tokens:
        tt = token.token_type
        if tt == TokenType.NUMBER:
            stack.append(token.data)
        elif tt == TokenType.VARIABLE:
            stack.append(VARIABLES[token.data])
        elif tt == TokenType.OPERATOR:
            if token.data == 'NEG':
                assert len(stack) >= 1
                v = stack.pop()
                assert isinstance(v, (int, float))
                stack.append(-v)
            elif token.data == '*':
                assert len(stack) >= 2
                rhs = stack.pop()
                lhs = stack.pop()
                assert isinstance(rhs, (int, float))
                assert isinstance(lhs, (int, float))
                stack.append(lhs * rhs)
            elif token.data == '/':
                assert len(stack) >= 2
                rhs = stack.pop()
                lhs = stack.pop()
                assert isinstance(rhs, (int, float))
                assert isinstance(lhs, (int, float))
                stack.append(lhs / rhs)
            elif token.data == '-':
                assert len(stack) >= 2
                rhs = stack.pop()
                lhs = stack.pop()
                assert isinstance(rhs, (int, float))
                assert isinstance(lhs, (int, float))
                stack.append(lhs - rhs)
            elif token.data == '+':
                assert len(stack) >= 2
                rhs = stack.pop()
                lhs = stack.pop()
                assert isinstance(rhs, (int, float))
                assert isinstance(lhs, (int, float))
                stack.append(lhs + rhs)
            else:
                raise Exception('Invalid operator %s' % token.data)
        elif tt == TokenType.COMMA:
            stack.append(',')
        elif tt == TokenType.FUNCTION:
            stack.append(FUNCTIONS[token.data](stack))
        else:
            raise Exception('Unrecognized token %s' % token)

        print(stack)

    if len(stack) != 1:
        raise Exception('Unexpected stack: %s' % stack)

    return stack[0]


def main(args):
    cs = CharStream(args.expression)

    # Tokenize.
    tokens = []
    while True:
        t = cs.pop_token()
        if t is None:
            break

        tokens.append(t)
    print(tokens)

    tokens = process_tokens_1(tokens)
    print(tokens)

    tokens = process_tokens_2(tokens)
    print(tokens)

    tokens = shunting_yard_tokens(tokens)
    print(tokens)

    v = evaluate_tokens(tokens)
    print(v)


def _main():
    print(sys.argv)
    parser = argparse.ArgumentParser()
    parser.add_argument('--expression', required=True)
    main(parser.parse_args())


if __name__ == '__main__':
    _main()
