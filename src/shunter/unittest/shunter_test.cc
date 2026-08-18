// Copyright (c) 2026 by Terry Greeniaus.
// All rights reserved.
#include "../shunting_yard.h"
#include <hdr/types.h>
#include <tmock/tmock.h>

static void
shunt_sin(std::vector<double>& stack)
{
    kassert(stack.size() >= 1);
    double v = stack.back();
    stack.pop_back();
    stack.push_back(sin(v));
}

class tmock_test
{
    TMOCK_TEST(test_shunter)
    {
        shunting_yard::shunter sys("-a * sin(12*y + 17) - 5 + a");
        const std::string tokens[] =
        {
            "VARIABLE(a)",
            "NEGATE",
            "12.000000",
            "VARIABLE(y)",
            "*",
            "17.000000",
            "+",
            "FUNCTION(sin)",
            "*",
            "5.000000",
            "-",
            "VARIABLE(a)",
            "+",
        };
        tmock::assert_equiv(sys.tokens.size(),NELEMS(tokens));
        for (size_t i=0; i<NELEMS(tokens); ++i)
            tmock::assert_equiv(sys.tokens[i].to_string(),tokens[i]);

        const std::string variables[] =
        {
            "a",
            "y",
        };
        tmock::assert_equiv(sys.variables.size(),NELEMS(variables));
        for (size_t i=0; i<NELEMS(variables); ++i)
            tmock::assert_equiv(sys.variables[i].name,variables[i]);

        const std::string functions[] =
        {
            "sin",
        };
        tmock::assert_equiv(sys.functions.size(),NELEMS(functions));
        for (size_t i=0; i<NELEMS(functions); ++i)
            tmock::assert_equiv(sys.functions[i].name,functions[i]);

        sys.variables[0].value = 1.23;
        sys.variables[1].value = 3.45;
        sys.functions[0].func = shunt_sin;
        tmock::assert_equiv(sys.evaluate(),-4.9519158547031115);
    }
};

TMOCK_MAIN();
