// Copyright (c) 2026 by Terry Greeniaus.
// All rights reserved.
#ifndef __SHUNTER_SHUNTING_YARD_H
#define __SHUNTER_SHUNTING_YARD_H

#include <hdr/kassert.h>
#include <libtsdb/exception.h>
#include <strutil/strutil.h>
#include <vector>
#include <algorithm>
#include <math.h>

namespace shunting_yard
{
    enum token_type
    {
        OPEN_PAREN,
        CLOSE_PAREN,
        OPERATOR,
        NEGATE,
        COMMA,
        NUMBER,
        STRING,
        FUNCTION,
        VARIABLE,
    };

    struct token
    {
        token_type  type;
        const char* pos;
        size_t      len;
        size_t      index;
        union
        {
            double      value;
            struct
            {
                uint8_t precedence;
                char    associativity;
            };
        };

        std::string to_string() const
        {
            switch (type)
            {
                case OPEN_PAREN:
                    return "(";

                case CLOSE_PAREN:
                    return ")";

                case OPERATOR:
                    return std::string(pos,1);

                case NEGATE:
                    return "NEGATE";

                case COMMA:
                    return ",";

                case NUMBER:
                    return str::printf("%f",value);

                case STRING:
                    return str::printf("STRING(%s)",
                                       std::string(pos,len).c_str());

                case FUNCTION:
                    return str::printf("FUNCTION(%s)",
                                       std::string(pos,len).c_str());

                case VARIABLE:
                    return str::printf("VARIABLE(%s)",
                                       std::string(pos,len).c_str());
            }
            __builtin_unreachable();
        }
    };

    // Check if a character can be the start of a word.
    static constexpr bool is_word_lead_char(char c)
    {
        switch (c)
        {
            case 'a' ... 'z':
            case 'A' ... 'Z':
            case '_':
                return true;

            default:
                return false;
        }
    }

    // Check if a character can be an interior part of a word.
    static constexpr bool is_word_interior_char(char c)
    {
        switch (c)
        {
            case '0' ... '9':
                return true;

            default:
                return is_word_lead_char(c);
        }
    }

    struct token_stream
    {
        // Parser state.
        const char*         data;
        const size_t        len;
        size_t              pos;

        // Pops a double-precision number which can be encoded using scientific
        // notation.
        double pop_number()
        {
            // Get the integer part.
            char c;
            uint64_t ipart = 0;
            size_t ndigits = 0;
            bool ipart_done = false;
            while (pos < len && !ipart_done)
            {
                c = data[pos];
                switch (c)
                {
                    case '.':
                    case 'e':
                        ipart_done = true;
                    break;

                    case '+':
                    case '-':
                    case '*':
                    case '/':
                    case ')':
                    case ',':
                    case ' ':
                    case '\t':
                        if (!ndigits)
                            throw tsdb::invalid_number_exception();
                        return ipart;
                    break;

                    case '0' ... '9':
                        ++pos;
                        ++ndigits;
                        ipart = 10*ipart + c - '0';
                    break;

                    default:
                        throw tsdb::invalid_number_exception();
                    break;
                }
            }
            if (pos >= len)
            {
                if (!ndigits)
                    throw tsdb::invalid_number_exception();
                return ipart;
            }

            // Get the fractional part and divisor.
            uint64_t fpart = 0;
            uint64_t fdiv = 1;
            if (c == '.')
            {
                ++pos;
                bool fpart_done = false;
                while (pos < len && !fpart_done)
                {
                    c = data[pos];
                    switch (c)
                    {
                        case 'e':
                            fpart_done = true;
                        break;

                        case '+':
                        case '-':
                        case '*':
                        case '/':
                        case ')':
                        case ',':
                        case ' ':
                        case '\t':
                            if (!ndigits)
                                throw tsdb::invalid_number_exception();
                            return ipart + (double)fpart / (double)fdiv;
                        break;

                        case '0' ... '9':
                            ++pos;
                            ++ndigits;
                            fpart = 10*fpart + c - '0';
                            fdiv *= 10;
                        break;

                        default:
                            throw tsdb::invalid_number_exception();
                        break;
                    }
                }
            }
            if (!ndigits)
                throw tsdb::invalid_number_exception();
            if (pos >= len)
                return ipart + (double)fpart / (double)fdiv;

            // Get the exponent.
            kassert(c == 'e');
            if (++pos >= len)
                throw tsdb::invalid_number_exception();
            int64_t exponent = 0;
            int64_t sign = 1;
            ndigits = 0;
            c = data[pos];
            switch (c)
            {
                // Handle a sign.
                case '-':
                    sign = -1;
                case '+':
                    if (++pos >= len)
                        throw tsdb::invalid_number_exception();
                break;

                case '0' ... '9':
                break;

                default:
                    throw tsdb::invalid_number_exception();
                break;
            }
            bool exponent_done = false;
            while (pos < len && !exponent_done)
            {
                c = data[pos];
                switch (c)
                {
                    case '+':
                    case '-':
                    case '*':
                    case '/':
                    case '(':
                    case ')':
                    case ',':
                    case ' ':
                    case '\t':
                        exponent_done = true;
                    break;

                    case '0' ... '9':
                        ++pos;
                        ++ndigits;
                        exponent = 10*exponent + c - '0';
                    break;

                    default:
                        throw tsdb::invalid_number_exception();
                    break;
                }
            }
            if (!ndigits)
                throw tsdb::invalid_number_exception();
            exponent *= sign;
            return (ipart + (double)fpart / (double)fdiv) * pow(10,exponent);
        }

        // Pops a word from the stream, returning the number of characters in
        // the word from the current position and then incrementing past it.
        size_t pop_word()
        {
            kassert(pos < len);
            kassert(is_word_lead_char(data[pos]));

            size_t p0 = pos++;
            while (pos < len && is_word_interior_char(data[pos]))
                ++pos;

            return pos - p0;
        }

        // Pops a token from the stream.  Returns true if a token was popped,
        // or false if we are at the end of the stream.
        bool pop_token(token* t)
        {
            // Advance to the start of the next token.
            while (pos < len && (data[pos] == ' ' || data[pos] == '\t'))
                ++pos;

            // If we are at the end of the string, no tokens are left.
            if (pos >= len)
                return false;

            // Handle a token.
            char c = data[pos];
            t->pos = data + pos;
            t->len = 1;
            t->index = 0;
            switch (c)
            {
                case '(':
                    ++pos;
                    t->type = OPEN_PAREN;
                    return true;
                break;

                case ')':
                    ++pos;
                    t->type = CLOSE_PAREN;
                    return true;
                break;

                case ',':
                    ++pos;
                    t->type = COMMA;
                    return true;
                break;

                case '+':
                case '-':
                    ++pos;
                    t->type = OPERATOR;
                    t->precedence = 2;
                    t->associativity = 'L';
                    return true;
                break;

                case '/':
                case '*':
                    ++pos;
                    t->type = OPERATOR;
                    t->precedence = 3;
                    t->associativity = 'L';
                    return true;
                break;

                case '0' ... '9':
                case '.':
                    t->type = NUMBER;
                    t->value = pop_number();
                    t->len = data + pos - t->pos;
                    return true;
                break;

                case 'a' ... 'z':
                case 'A' ... 'Z':
                case '_':
                    t->type = STRING;
                    t->len = pop_word();
                    return true;
                break;

                default:
                    throw tsdb::invalid_formula_character_exception();
                break;
            }
        }

        constexpr token_stream(const char* data, size_t len):
            data(data),
            len(len),
            pos(0)
        {
        }
    };

    struct variable
    {
        const std::string   name;
        double              value;
    };

    struct function
    {
        const std::string   name;
        void                (*func)(std::vector<double>& stack);
    };

    struct shunter
    {
        // Final list of shunted tokens.
        std::vector<token>  tokens;

        // Intermediate token lists.
        std::vector<token>  tokens_1;
        std::vector<token>  tokens_2;

        // Variable and function lists.
        std::vector<variable>   variables;
        std::vector<function>   functions;

        // First pass over the tokens list.  Convert STRING tokens to FUNCTION
        // or VARIABLE.  Convert OPERATOR '-' to 'NEG' where appropriate.
        // Discard OPERTOR '+' where is is just a sign character.  Discard 'NEG'
        // pairs.
        void process_tokens_1()
        {
            for (const token& t : tokens_1)
            {
                // If we have a previous token, use it to figure out what can
                // come next.
                if (!tokens_2.empty())
                {
                    token& pt = tokens_2.back();
                    token_type ptt = pt.type;
                    switch (ptt)
                    {
                        case CLOSE_PAREN:
                        case NUMBER:
                        case VARIABLE:
                            switch (t.type)
                            {
                                case CLOSE_PAREN:
                                case OPERATOR:
                                case COMMA:
                                    tokens_2.push_back(t);
                                    continue;
                                break;

                                default:
                                    throw tsdb::unexpected_token_exception();
                                break;
                            }
                        break;

                        case STRING:
                        {
                            std::string name(pt.pos,pt.len);

                            if (t.type == OPEN_PAREN)
                            {
                                auto it = std::find_if(
                                    functions.begin(),functions.end(),
                                    [&name](const function& v)
                                    {
                                        return v.name == name;
                                    }
                                );

                                pt.type = FUNCTION;
                                if (it != functions.end())
                                    pt.index = it - functions.begin();
                                else
                                {
                                    pt.index = functions.size();
                                    functions.push_back({name,NULL});
                                }
                            }
                            else
                            {
                                auto it = std::find_if(
                                    variables.begin(),variables.end(),
                                    [&name](const variable& v)
                                    {
                                        return v.name == name;
                                    }
                                );

                                pt.type = VARIABLE;
                                if (it != variables.end())
                                    pt.index = it - variables.begin();
                                else
                                {
                                    pt.index = variables.size();
                                    variables.push_back({name,0.});
                                }
                            }
                            tokens_2.push_back(t);
                            continue;
                        }
                        break;

                        case NEGATE:
                            if (ptt == NEGATE)
                            {
                                if (t.type == OPERATOR)
                                {
                                    if (*t.pos == '-')
                                    {
                                        tokens_2.pop_back();
                                        continue;
                                    }
                                    if (*t.pos == '+')
                                        continue;
                                }
                                else if (t.type == NUMBER)
                                {
                                    tokens_2.pop_back();
                                    tokens_2.push_back(token{NUMBER,t.pos,t.len,
                                                             0,{-t.value}});
                                    continue;
                                }
                            }
                        break;

                        default:
                        break;
                    }
                }

                // There are no previous tokens, or the last token was an open-
                // paren, a math operator or a comma.  We can either have
                // another open-paren, a number (possibly preceded by one or
                // more sign characters), or a string.  This is the start of an
                // expression.
                if (t.type == OPERATOR)
                {
                    if (*t.pos == '+')
                        continue;
                    else if (*t.pos == '-')
                    {
                        token nt{NEGATE,t.pos,t.len};
                        nt.precedence = 4;
                        nt.associativity = 'R';
                        tokens_2.push_back(nt);
                        continue;
                    }
                }
                tokens_2.push_back(t);
            }

            // If the final token was a string, it must be a variable.
            if (!tokens_2.empty() && tokens_2.back().type == STRING)
            {
                token& pt = tokens_2.back();
                std::string name(pt.pos,pt.len);
                pt.type = VARIABLE;

                auto it = std::find_if(
                    variables.begin(),variables.end(),
                    [&name](const variable& v)
                    {
                        return v.name == name;
                    }
                );

                if (it != variables.end())
                    pt.index = it - variables.begin();
                else
                {
                    pt.index = variables.size();
                    variables.push_back({name,0.});
                }
            }
        }

        void shunt_tokens()
        {
            std::vector<token> operator_stack;
            for (const token& t : tokens_2)
            {
                switch (t.type)
                {
                    case NUMBER:
                    case VARIABLE:
                        tokens.push_back(t);
                    break;

                    case FUNCTION:
                    case OPEN_PAREN:
                        operator_stack.push_back(t);
                    break;

                    case OPERATOR:
                    case NEGATE:
                        while (!operator_stack.empty())
                        {
                            const token& opt = operator_stack.back();
                            if (opt.type == OPEN_PAREN)
                                break;
                            if (opt.type != OPERATOR && opt.type != NEGATE)
                                throw tsdb::unexpected_token_exception();

                            if (opt.precedence > t.precedence ||
                                (opt.precedence == t.precedence &&
                                 t.associativity == 'L'))
                            {
                                tokens.push_back(opt);
                                operator_stack.pop_back();
                                continue;
                            }

                            break;
                        }

                        operator_stack.push_back(t);
                    break;

                    case COMMA:
                        while (!operator_stack.empty())
                        {
                            const token& opt = operator_stack.back();
                            if (opt.type == OPEN_PAREN)
                                break;

                            if (opt.type != OPERATOR && opt.type != NEGATE)
                                throw tsdb::unexpected_token_exception();

                            tokens.push_back(opt);
                            operator_stack.pop_back();
                        }
                        tokens.push_back(t);
                    break;

                    case CLOSE_PAREN:
                        while (!operator_stack.empty())
                        {
                            const token& opt = operator_stack.back();
                            if (opt.type == OPEN_PAREN)
                                break;
                            
                            tokens.push_back(opt);
                            operator_stack.pop_back();
                        }

                        // Either the stack is empty or we found an open paren
                        // that matches this closing paren.
                        if (operator_stack.empty())
                            throw tsdb::missing_paren_exception();
                        operator_stack.pop_back();

                        if (!operator_stack.empty() &&
                            operator_stack.back().type == FUNCTION)
                        {
                            tokens.push_back(operator_stack.back());
                            operator_stack.pop_back();
                        }
                    break;

                    case STRING:
                        kabort();
                    break;
                }
            }

            while (!operator_stack.empty())
            {
                // We have run through all tokens, which means we have
                // processed all closing parens.  If there is an open paren,
                // then it is unmatched.
                if (operator_stack.back().type == OPEN_PAREN)
                    throw tsdb::missing_paren_exception();
                tokens.push_back(operator_stack.back());
                operator_stack.pop_back();
            }
        }

        double evaluate() const
        {
            std::vector<double> stack;
            double v;
            for (const token& t : tokens)
            {
                switch (t.type)
                {
                    case NUMBER:
                        stack.push_back(t.value);
                    break;

                    case VARIABLE:
                        stack.push_back(variables[t.index].value);
                    break;

                    case FUNCTION:
                        functions[t.index].func(stack);
                    break;

                    case NEGATE:
                        if (stack.empty())
                            throw tsdb::shunt_missing_op_arg_exception();
                        v = stack.back();
                        stack.pop_back();
                        stack.push_back(-v);
                    break;

                    case OPERATOR:
                    {
                        if (stack.size() < 2)
                            throw tsdb::shunt_missing_op_arg_exception();
                        double rhs = stack.back();
                        stack.pop_back();
                        double lhs = stack.back();
                        stack.pop_back();

                        switch (*t.pos)
                        {
                            case '*': stack.push_back(lhs * rhs); break;
                            case '/': stack.push_back(lhs / rhs); break;
                            case '-': stack.push_back(lhs - rhs); break;
                            case '+': stack.push_back(lhs + rhs); break;

                            default:
                                kabort();
                            break;
                        }
                    }
                    break;

                    case COMMA:
                        // Just ignore it for now.
                    break;

                    default:
                        kabort();
                    break;
                }
            }

            if (stack.size() != 1)
                throw tsdb::shunt_extra_expressions_exception();
            return stack[0];
        }

        std::string to_string() const
        {
            std::string s = "[";

            for (const auto& t : tokens)
            {
                s += " ";
                s += t.to_string();
            }

            return s + " ]";
        }

        shunter(const char* data, size_t len)
        {
            token_stream ts(data,len);
            token t;
            while (ts.pop_token(&t))
                tokens_1.push_back(t);
            process_tokens_1();
            shunt_tokens();
        }

        shunter(const char* str):shunter(str,strlen(str)) {}

        shunter(const std::string& str):shunter(&str[0],str.size()) {}
    };
}

#endif /* __SHUNTER_SHUNTING_YARD_H */
