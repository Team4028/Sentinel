import yaml
from enum import Enum
from typing import List
import pandas as pd


def read_config(year: str):
    """Reads the configuration YAML file into memory"""
    try:
        with open(f"config/field-config-{year}.yaml") as f:
            data = yaml.safe_load(f)
        return data
    except FileNotFoundError:
        print("Error: File not found at path")
    except yaml.YAMLError as e:
        print(f"YAML Parser encountered an error: {e}")


FILTERS = ["avg", "max", "fil"]

# more human readable names for the yaml filters
FANCY_FIL = {"avg": "Average", "max": "Max", "fil": "Filtered"}


class GrafanaDataPreset(Enum):
    MATCH = lambda data: zip(
        [x["name"] for x in data["match-fields"]],
        ["number" for _ in data["match-fields"]],
    )
    TEAM = lambda data: [("Rank", "number"), ("Average RP", "number"), ("OPR", "number"), ("Last OPR", "number")] + list(zip(
        [
            FANCY_FIL[f] + " " + x["name"]
            for f in FILTERS
            for x in data["team-fields"]
            if f in x
        ],
        ["number" for __ in FILTERS for _ in data["team-fields"] if __ in _],
    ))  # fill in the blanks type for loop

    PREMATCH = lambda data: zip(
        [x["name"] for x in data["depth-predict-fields"]],
        ["number" for _ in data["depth-predict-fields"]],
    )

    PREMATCH_SCORE = lambda _: [
        ("1 Score", "number"),
        ("2 Score", "number"),
        ("3 Score", "number"),
        ("Won", "boolean"),
    ]

    NONE = lambda _: []


GRAFANA_DATA_PANELS = {
    "ScoutingDashboard.json": {
        "Match Predictions": GrafanaDataPreset.PREMATCH_SCORE,
        "Team Summary": GrafanaDataPreset.PREMATCH,
        "Team Data": GrafanaDataPreset.TEAM,
        "Match View": GrafanaDataPreset.MATCH,
        "Team Compare": GrafanaDataPreset.TEAM,
    },
    "TeamView.json": {"Team View": GrafanaDataPreset.TEAM},
}


def lex_config(year: str):
    """reads and restructures the YAML for use by the Processor"""
    data = read_config(year)
    config = {
        "compute": [],
        "headers": [],
        "teams": [],
        "matches": [],
        "predict-metric": "",
        "dash-panel": {},
        "deep-predict": [],
    }
    if data:
        for k, v in GRAFANA_DATA_PANELS.items():
            config["dash-panel"][k] = {}
            for ki, vi in v.items():  # (i for inner)
                config["dash-panel"][k][ki] = vi(data)
        for field in data["headers"]:
            config["headers"].append(field["name"])
        for field in data["compute-fields"]:
            config["compute"].append({"name": field["name"], "eq": field["equation"]})
        for field in data["team-fields"]:
            config["teams"].append(
                {
                    "name": field["name"],
                    "filters": [x for x in FILTERS if x in field],
                    "derive": field["derive"],
                }
            )
        for field in data["match-fields"]:
            config["matches"].append(
                {
                    "name": field["name"],
                    "derive": field["derive"],
                    "filters": [x for x in FILTERS if x in field],
                }
            )
        config["p-metric"] = data["predict-metric"]
        for field in data["depth-predict-fields"]:
            config["deep-predict"].append(
                {"name": field["name"], "source": field["source"]}
            )
    return config


# ========= BEAKSCRIPT =========
class TOKENS(Enum):  # different types of expressions there are
    BINARY_OP = 0
    UNARY_OP = 1
    LITERAL = 2
    HEADER = 3
    HEADER_COND = 4
    PAREN = 5
    LIST_LITERAL = 6


class Token:  # represents a thing that the lexer can match to
    token: TOKENS
    symbol: str | pd.DataFrame

    def __init__(self, token, symbol):
        self.token = token
        self.symbol = symbol

    def __str__(self):
        return f"{self.token.name}: {self.symbol}"

    def __repr__(self):
        return f"{self.token.name}: {self.symbol}"


# all unary operators
UNOPS = [
    "-",
    "!" "@avg",
    "@max",
    "@min",
    "@sum",
    "@len",
]

# all binary operators
OPERATOR_STRINGS = ["+", "-", "*", "/", "%", ">", "<", "=", "!", "&", "|", "^", "`"]

# all comparison operators that can be extended by adding an = (ie. > to >=)
COMP_EXTEND = [">", "<", "!", "="]

# the parentheses because i am too lazy to type out ["(", ")"] a bunch
PARENS = ["(", ")"]

# same as above
LIST_LITERALS = ["{", "}"]

# unary operator precedence list (precedence = which goes first)
UNOP_PRECEDENCE = {
    "-": 7,
    "!": 7,
    "@sum": 7,
    "@avg": 7,
    "@max": 7,
    "@min": 7,
    "@len": 7,
}

# binary operator precedence list
BIOP_PRECEDENCE = {
    "[]": 10,
    "*": 6,
    "/": 6,
    "%": 6,
    "+": 5,
    "-": 5,
    "`": 4,
    ">": 4,
    "<": 4,
    ">=": 4,
    "<=": 4,
    "!": 3,
    "!=": 3,
    "=": 3,
    "==": 3,
    "^": 2,
    "&": 2,
    "|": 1,
}


def get_last_nowhitespace(s, i):
    """get the last character that wasn't whitespace"""
    j = i - 1
    while j >= 0 and s[j].isspace():
        j -= 1
    return s[j] if j >= 0 else None


def parse_equation(equation: str, df: pd.DataFrame):
    equation_tokens = []
    equation = equation.strip()
    i = 0
    while i < len(equation):
        c: str = equation[i]
        # skip spaces
        if c.isspace():
            i += 1
            continue
        # if @, read next three letters for unop
        if c == "@":
            if i + 3 < len(equation):
                equation_tokens.append(
                    Token(TOKENS.UNARY_OP, "".join(equation[i + j] for j in range(4)))
                )
                i += 4
            else:
                i += 1
            continue
        # if paren, read paren
        if c in PARENS:
            equation_tokens.append(Token(TOKENS.PAREN, c))
        # if bracket, read bracket
        elif c == "[" or c == "]":
            equation_tokens.append(Token(TOKENS.HEADER_COND, c))
        # if list (too hard to read list and do elsewhere), read each list index and evaluate the inside
        elif c == "{":
            i += 1
            list_tokens = []
            curr = ""
            b_count = 1
            p_count = 0
            while i < len(equation):
                c = equation[i]
                if c == "(":
                    p_count += 1
                elif c == ")":
                    p_count -= 1
                elif c == "{":
                    b_count += 1
                    curr += c
                elif c == "}":
                    b_count -= 1
                    if b_count == 0:
                        if curr.strip():
                            list_tokens.append(curr.strip())
                        break
                    else:
                        curr += c
                elif (
                    c == "," and b_count == 1 and p_count == 0
                ):  # this way you can do ($A,B) without making two entries in the list
                    # make new entry
                    list_tokens.append(curr.strip())
                    curr = ""
                else:
                    curr += c  # add to current entry
                i += 1
            literal_list = [
                eval_beakscript(item, df) for item in list_tokens
            ]  # evaluate entries
            # make the list into a pd.Series list so it can be filtered
            equation_tokens.append(Token(TOKENS.LITERAL, pd.Series(literal_list)))
        # if its an operator
        elif c in OPERATOR_STRINGS:
            # check if its unary (at beginning, right after other operator or beginning of {/[/(, or right after @xxx operator)
            if (
                i == 0
                or get_last_nowhitespace(equation, i) in OPERATOR_STRINGS
                or equation[i - 1] in ["(", "[", "{"]
                or (i - 4 >= 0 and equation[i - 4] == "@")
            ):
                equation_tokens.append(Token(TOKENS.UNARY_OP, c))
            # turn < into <=, etc
            elif equation[i] in COMP_EXTEND:
                if i + 1 < len(equation) and equation[i + 1] == "=":
                    equation_tokens.append(Token(TOKENS.BINARY_OP, c + "="))
                    i += 1
                else:
                    equation_tokens.append(Token(TOKENS.BINARY_OP, c))
            # otherwise just add it
            else:
                equation_tokens.append(Token(TOKENS.BINARY_OP, c))
        # its a header!
        elif c == "$":
            if i < len(equation):
                i += 1  # don't add $ to the header name
            else:
                break
            ref_name = ""
            while (
                (i < len(equation))
                and (not equation[i] == "$")
                and (
                    not equation[i]
                    in ["$", "[", "]", "@", *OPERATOR_STRINGS, *PARENS, *LIST_LITERALS]
                )
            ):
                ref_name += equation[i]  # read the header
                i += 1
            equation_tokens.append(Token(TOKENS.HEADER, ref_name.strip()))
            continue
        # its a literal
        else:
            literal_value = ""
            if i >= len(equation):
                break
            while (
                (i < len(equation))
                and (not equation[i] == "$")
                and (
                    not equation[i]
                    in ["$", "[", "]", "@", *OPERATOR_STRINGS, *PARENS, *LIST_LITERALS]
                )
            ):
                literal_value += equation[i]  # read the literal
                i += 1
            equation_tokens.append(Token(TOKENS.LITERAL, literal_value.strip()))
            continue
        i += 1
    return equation_tokens


# === TYPE COERCION ===
def floatize_if_str(x):
    """converts x to a float if it's a string and it can"""
    try:
        return float(x) if type(x) == str else x
    except ValueError:
        return x


def strize_if_float(x):
    """converts x to a string if it's a float and it can"""
    try:
        return str(x) if type(x) == float else x
    except ValueError:
        return x


def strfloatize_if_bool(x):
    """converts a bool to a '1' for true and a '0' for false, a string representation of a float-compatible encoding of the boolean"""
    if type(x) == bool:
        return "1" if x else "0"
    return x

def df_safe_in(a, b):
    try:
        return b.str.contains(a, na=False)
    except:
        return a in b 


def df_safe_and(a, b):
    """try and use & for dataframes, else use normal and"""
    try:
        return a & b
    except:
        return bool(a) and bool(b)


def df_safe_or(a, b):
    """try and use | for dataframes, else use normal or"""
    try:
        return a | b
    except:
        return bool(a) or bool(b)


# =====================


def evaluate_unary_operator(x, op):
    """evaluates `op` on `x`, trying its best to return a string"""
    match (op):
        case "-":
            return strize_if_float(-x)
        case "!":
            if type(x) == pd.Series:
                return ~x
            return strfloatize_if_bool(not x)
        case "@avg":
            try:
                return strize_if_float(x.mean())  # pd
            except:
                try:
                    return strize_if_float(sum(x) / len(x))  # list
                except:
                    return strize_if_float(x)  # else
        case "@max":
            try:
                return strize_if_float(x.max())  # pd
            except:
                try:
                    return strize_if_float(max(x))  # list
                except:
                    return strize_if_float(x)
        case "@min":
            try:
                return strize_if_float(x.min())
            except:
                try:
                    return strize_if_float(min(x))
                except:
                    return strize_if_float(x)
        case "@sum":
            try:
                return strize_if_float(x.sum(axis=1))
            except:
                try:
                    return strize_if_float(sum(x))
                except:
                    return strize_if_float(x)
        case "@len":
            try:
                return strize_if_float(len(x))
            except:
                strize_if_float(x)
    print(f"Parser Error: Unknown unary operation: {op}")
    return "nan"


def evaluate_binary_operator(lhs, rhs, op):
    """evalutaes `lhs op rhs`, trying its best to return a string"""
    match (op):
        case "+":
            return strize_if_float(lhs + rhs)
        case "-":
            return strize_if_float(lhs - rhs)
        case "*":
            return strize_if_float(lhs * rhs)
        case "/":
            return strize_if_float(lhs / rhs)
        case "%":
            return strize_if_float(lhs % rhs)
        case "`":
            return strfloatize_if_bool(df_safe_in(lhs, rhs))
        case ">":
            return strfloatize_if_bool(lhs > rhs)
        case "<":
            return strfloatize_if_bool(lhs < rhs)
        case ">=":
            return strfloatize_if_bool(lhs >= rhs)
        case "<=":
            return strfloatize_if_bool(lhs <= rhs)
        case "=" | "==":
            return strfloatize_if_bool(lhs == rhs)
        case "!" | "!=":
            return strfloatize_if_bool(lhs != rhs)
        case "^":
            return lhs ^ rhs
        case "&":
            return strfloatize_if_bool(df_safe_and(lhs, rhs))
        case "|":
            return strfloatize_if_bool(df_safe_or(lhs, rhs))
        case "[]":
            return lhs.loc[rhs]
    print(f"Parser error: Unexpected binary operator: {op}")
    return "nan"


def preproc_implicit_ops(tokens: List[Token]):
    """Convert HEADER_COND tokens <h[expr]> into binary operators h [] (expr) to make rpn easier"""
    output = []
    i = 0
    while i < len(tokens):
        t = tokens[i]
        if t.token == TOKENS.HEADER:
            output.append(t)

            if (
                i + 1 < len(tokens)
                and tokens[i + 1].token == TOKENS.HEADER_COND
                and tokens[i + 1].symbol == "["
            ):
                i += 2
                inner = []
                b_count = 1
                while i < len(tokens):
                    if tokens[i].token == TOKENS.HEADER_COND:
                        if tokens[i].symbol == "[":
                            b_count += 1
                        else:
                            b_count -= 1
                            if b_count == 0:
                                break
                    inner.append(tokens[i])
                    i += 1
                # use parens to make sure whats inside the brackets gets evaluated first
                output.append(Token(TOKENS.BINARY_OP, "[]"))
                output.append(Token(TOKENS.PAREN, "("))
                output.extend(preproc_implicit_ops(inner))
                output.append(Token(TOKENS.PAREN, ")"))
        elif (
            t.token == TOKENS.HEADER_COND
            and i > 0
            and (type(tokens[i - 1].symbol) == pd.Series or tokens[i - 1].symbol == ")")
        ):
            i += 1
            inner = []
            b_count = 1
            while i < len(tokens):
                if tokens[i].token == TOKENS.HEADER_COND:
                    if tokens[i].symbol == "[":
                        b_count += 1
                    else:
                        b_count -= 1
                        if b_count == 0:
                            break
                inner.append(tokens[i])
                i += 1
            # use parens to make sure whats inside the brackets gets evaluated first
            output.append(Token(TOKENS.BINARY_OP, "[]"))
            output.append(Token(TOKENS.PAREN, "("))
            output.extend(preproc_implicit_ops(inner))
            output.append(Token(TOKENS.PAREN, ")"))
        else:
            output.append(t)

        i += 1
    return output


def rpn(tokens: List[Token]):
    """Converts `tokens` into a list of reverse polish notation tokens to make parsing much easier"""
    output = []
    ops: List[Token] = []

    for token in tokens:
        if token.token == TOKENS.LITERAL:
            output.append(token)
        elif token.token in [TOKENS.UNARY_OP, TOKENS.BINARY_OP]:
            if token.token == TOKENS.UNARY_OP:
                prec_list = UNOP_PRECEDENCE
                l_assoc = False
            else:
                prec_list = BIOP_PRECEDENCE
                l_assoc = True
            while (
                ops
                and ops[-1].token in [TOKENS.UNARY_OP, TOKENS.BINARY_OP]
                and (
                    (
                        (
                            UNOP_PRECEDENCE
                            if ops[-1].token == TOKENS.UNARY_OP
                            else BIOP_PRECEDENCE
                        )[ops[-1].symbol]
                        >= prec_list[token.symbol]
                    )
                    if l_assoc
                    else (
                        (
                            UNOP_PRECEDENCE
                            if ops[-1].token == TOKENS.UNARY_OP
                            else BIOP_PRECEDENCE
                        )[ops[-1].symbol]
                        > prec_list[token.symbol]
                    )
                )
            ):
                output.append(
                    ops.pop()
                )  # append other operator first if it has higher precedence
            ops.append(token)
        elif token.token == TOKENS.PAREN and token.symbol:
            if token.symbol == "(":
                ops.append(Token(TOKENS.PAREN, "("))
            else:
                while ops and not (
                    ops[-1].token == TOKENS.PAREN and ops[-1].symbol == "("
                ):
                    output.append(ops.pop())
                ops.pop()
        elif token.token == TOKENS.HEADER or token.token == TOKENS.HEADER_COND:
            output.append(token)

    while ops:
        output.append(ops.pop())

    return list(output)


def solve_rpn(rpn_tokens: List[Token], df: pd.DataFrame):
    """parses `rpn_tokens`, using `df` to evaluate the headers"""
    stack_overflow = []  # the stack
    for t in rpn_tokens:
        tok = t.token
        sym = t.symbol

        if tok == TOKENS.HEADER:
            if "," in sym:  # multi-header = sum
                stack_overflow.append(
                    df[[s.strip() for s in sym.split(",")]].sum(axis=1)
                )
            else:
                stack_overflow.append(df[sym])
            continue

        if tok == TOKENS.LITERAL:
            stack_overflow.append(sym)
            continue

        if tok == TOKENS.UNARY_OP:
            val = floatize_if_str(
                stack_overflow.pop()
            )  # pop closest value to apply unop to
            stack_overflow.append(evaluate_unary_operator(val, sym))
            continue

        if tok == TOKENS.BINARY_OP:
            rhs = floatize_if_str(stack_overflow.pop())
            lhs = floatize_if_str(
                stack_overflow.pop()
            )  # pop operands (rhs first bc rpn notation) to apply biop to
            stack_overflow.append(evaluate_binary_operator(lhs, rhs, sym))
            continue

    if len(stack_overflow) != 1:
        raise ValueError(f"Error: operation: {stack_overflow} cannot be simplified.")
    ret = stack_overflow[0]
    try:
        if (
            type(ret) == pd.Series and ret.size == 1
        ):  # calling float on len 1 series will eventually throw an error, and we want to unwrap len 1 series.
            return float(ret.iloc[0])
        return float(ret)
    except:
        return ret


def eval_beakscript(equation: str, df: pd.DataFrame):
    tokens = parse_equation(equation, df)  # first parse the string
    tokens = preproc_implicit_ops(tokens)  # then restructure the brackets
    rpnResult = rpn(tokens)  # then convert the tokens to rpn
    return solve_rpn(rpnResult, df)  # then evaluate it


if __name__ == "__main__":
    # test beakscript
    print(
        eval_beakscript(
            "@sum$TN,MN[$TC == R | $TC = Y]",
            pd.DataFrame({"TN": [3, 5, 2], "MN": [2, 3, 8], "TC": ["R", "B", "Y"]}),
        )
    )

    print(
        eval_beakscript(
            "{($TN,MN), $TN - $MN}",
            pd.DataFrame({"TN": [3, 5, 2], "MN": [2, 3, 8], "TC": ["R", "B", "Y"]}),
        )
    )

    print(
        eval_beakscript(
            "$TN[$TC != Y]",
            pd.DataFrame({"TN": [3, 5, 2], "MN": [2, 3, 8], "TC": ["R", "B", "Y"]}),
        )
    )

    print(eval_beakscript("@len{2, 3, 5, 6}", {}))
    # had to keep this one around because I was impressed that it worked
    # it works because n is a literal, so @lenn = len(n) = 1, and 1 - {2, 3, 5, 6} becomes {1, 1, 1, 1} - {2, 3, 5, 6}, which becomes {-1, -2, -4, -5}
    print(eval_beakscript("@lenn-{2, 3, 5, 6}", {}))
