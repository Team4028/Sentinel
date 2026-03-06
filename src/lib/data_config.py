import yaml
from enum import Enum
import pandas as pd
import traceback
import re
import logging

logger = logging.getLogger(__name__)


def read_config(year: str):
    """Reads the configuration YAML file into memory"""
    try:
        with open(f"config/field-config-{year}.yaml") as f:
            data = yaml.safe_load(f)
        return data
    except FileNotFoundError:
        logger.error("Error: File not found at path")
    except yaml.YAMLError as e:
        logger.error(f"YAML Parser encountered an error: {e}")


FILTERS = ["avg", "max", "fil"]

SVD_AUGS = ["variance-score", "stability"]

# more human readable names for the yaml filters
FANCY_FIL = {"avg": "Average", "max": "Max", "fil": "Filtered"}


class GrafanaDataPreset(Enum):

    def get_svd_headers(svd) -> list[str]:
        x = [svd["name"]]
        if "variance-score" in svd:
            x.append(svd["name"] + " Variance")
        return x

    MATCH = lambda data: zip(
        [x["name"] for x in data["match-fields"]],
        ["number" for _ in data["match-fields"]],
    )
    TEAM = lambda data: ([
        ("Rank", "number"),
        ("Average RP", "number"),
        ("OPR", "number"),
        ("Last OPR", "number"),
    ]
    + list(zip(
        l := [_ for svd in data["subjective-svd-fields"] for _ in GrafanaDataPreset.get_svd_headers(svd)], # walrus to avoid reparse list
        ["number" for _ in l]
    )) if "subjective-svd-fields" in data else list()
    + list(
        zip(
            l := [
                FANCY_FIL[f] + " " + x["name"]
                for f in FILTERS
                for x in data["team-fields"]
                if f in x
            ],
            ["number" for _ in l],
        )
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
        "svd": [],
        "teams": [],
        "matches": [],
        "predict-metric": "",
        "uniques": [],
        "preproc": [],
        "dash-panel": {},
        "deep-predict": [],
        "tests": [],
    }
    if data:
        for k, v in GRAFANA_DATA_PANELS.items():
            config["dash-panel"][k] = {}
            for ki, vi in v.items():  # (i for inner)
                config["dash-panel"][k][ki] = vi(data)
        config["tn"] = data["team-header-name"]
        config["mn"] = data["match-header-name"]
        for field in data["headers"]:
            config["headers"].append(field["name"])
        if "preproc-operations" in data:
            for field in data["preproc-operations"]:
                _data = {"name": field["name"], "op": field["operation"]}
                if "new-headers" in field:
                    _data |= {"new-headers": field["new-headers"]}
                config["preproc"].append(_data)
        for field in data["compute-fields"]:
            config["compute"].append({"name": field["name"], "eq": field["equation"]})
        config["uniques"] = data["filter-unique-fields"]
        if "subjective-svd-fields" in data:
            for field in data["subjective-svd-fields"]:
                config["svd"].append(
                    {
                        "name": field["name"],
                        "source": field["source"],
                        "comp-team": field["compare-team-source"],
                        "augs": [x for x in SVD_AUGS if x in field],
                    }
                )
        for field in data["team-fields"]:
            config["teams"].append(
                {
                    "name": field["name"],
                    "filters": [x for x in FILTERS if x in field],
                    "derive": field["derive"],
                }
            )
        if "data-tests" in data:
            for test in data["data-tests"]:
                config["tests"].append(
                    {
                        "name": test["name"],
                        "expr": test["expression"],
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
    def __init__(self, token, symbol, column=0):
        self.token = token
        self.symbol = symbol
        self.column = column

    def __str__(self):
        return f"<{self.token.name}: {self.symbol}, col={self.column}>"

    def __repr__(self):
        return f"<{self.token.name}: {self.symbol}, col={self.column}>"


class BeakscriptInterpretError(RuntimeError):
    curr_equation: str = ""
    curr_eq_name: str = ""

    def __str__(self):
        return f"[{BeakscriptInterpretError.curr_eq_name}] {super().__str__()}"

    ...


class UnpackList(list): ...


def float_or_int(x):
    fx = float(x)
    if fx % 1 == 0:
        return int(fx)
    return fx


# all unary operators
UNOPS = [
    "*",
    "-",
    "!",
    "@avg",
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
    "*": 7,
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
                if "".join(equation[i + j] for j in range(4)) not in UNOPS:
                    raise BeakscriptInterpretError(
                        f"Error at column {i + 1}: invalid unary operator expression after '@'\n{equation}\n{' ' * i + '^'}"
                    )
                equation_tokens.append(
                    Token(
                        TOKENS.UNARY_OP,
                        "".join(equation[i + j] for j in range(4)),
                        i + 1,
                    )
                )
                i += 4
            else:
                raise BeakscriptInterpretError(
                    f"Error at column {i + 1}: not enough characters after '@' unary operator declaration\n{equation}\n{' ' * i + '^'}"
                )
            continue
        # if paren, read paren
        if c in PARENS:
            equation_tokens.append(Token(TOKENS.PAREN, c, i + 1))
        # if bracket, read bracket
        elif c == "[" or c == "]":
            equation_tokens.append(Token(TOKENS.HEADER_COND, c, i + 1))
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
                    curr += c
                elif c == ")":
                    p_count -= 1
                    curr += c
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
                _
                for item in list_tokens
                for res in (eval_beakscript(item, df),)
                for _ in (res if isinstance(res, UnpackList) else (res,))
            ]  # evaluate entries
            # make the list into a pd.Series list so it can be filtered
            equation_tokens.append(
                Token(TOKENS.LITERAL, pd.Series(literal_list), i + 1)
            )
        # if its an operator
        elif c in OPERATOR_STRINGS:
            # check if its unary (at beginning, right after other operator or beginning of {/[/(, or right after @xxx operator)
            if (
                i == 0
                or get_last_nowhitespace(equation, i) in OPERATOR_STRINGS
                or equation[i - 1] in ["(", "[", "{"]
                or (i - 4 >= 0 and equation[i - 4] == "@")
            ):
                if c not in UNOPS:
                    raise BeakscriptInterpretError(
                        f"Error at column {i + 1}: invalid use of {c} as an unary operator\n{equation}\n{' ' * i + '^'}"
                    )
                equation_tokens.append(Token(TOKENS.UNARY_OP, c, i + 1))
            # turn < into <=, etc
            elif equation[i] in COMP_EXTEND:
                if i + 1 < len(equation) and equation[i + 1] == "=":
                    equation_tokens.append(Token(TOKENS.BINARY_OP, c + "=", i + 1))
                    i += 1
                else:
                    equation_tokens.append(Token(TOKENS.BINARY_OP, c, i + 1))
            # otherwise just add it
            else:
                if c not in BIOP_PRECEDENCE:
                    raise BeakscriptInterpretError(
                        f"Error at column {i + 1}: invalid use of {c} as an binary operator\n{equation}\n{' ' * i + '^'}"
                    )
                equation_tokens.append(Token(TOKENS.BINARY_OP, c, i + 1))
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
                    equation[i] not in
                    ["$", "[", "]", "@", *OPERATOR_STRINGS, *PARENS, *LIST_LITERALS]
                )
            ):
                ref_name += equation[i]  # read the header
                i += 1
            equation_tokens.append(Token(TOKENS.HEADER, ref_name.strip(), i + 1))
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
                    equation[i] not in
                    ["$", "[", "]", "@", *OPERATOR_STRINGS, *PARENS, *LIST_LITERALS]
                )
            ):
                literal_value += equation[i]  # read the literal
                i += 1
            equation_tokens.append(Token(TOKENS.LITERAL, literal_value.strip(), i + 1))
            continue
        i += 1
    return equation_tokens


# === TYPE COERCION ===
def floatize_if_str(x):
    """converts x to a float if it's a string and it can"""
    if isinstance(x, str) and "'" in x:
        return x.replace("'", "")
    try:
        return float_or_int(x) if isinstance(x, str) else x
    except ValueError:
        return x


def strize_if_float(x):
    """converts x to a string if it's a float and it can"""
    try:
        return str(x) if isinstance(x, float) else x
    except ValueError:
        return x


def strfloatize_if_bool(x):
    """converts a bool to a '1' for true and a '0' for false, a string representation of a float-compatible encoding of the boolean"""
    if isinstance(x, bool):
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


def evaluate_unary_operator(x, op, index):
    """evaluates `op` on `x`, trying its best to return a string"""
    match (op):
        case "*":
            if isinstance(x, pd.Series):
                return UnpackList(x.tolist())
            return x
        case "-":
            return strize_if_float(-x)
        case "!":
            if isinstance(x, pd.Series):
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
    raise BeakscriptInterpretError(
        f"Parser Error: Unknown unary operation: {op}\n{BeakscriptInterpretError.curr_equation}\n{' ' * index + '^'}"
    )


def evaluate_binary_operator(lhs, rhs, op, index):
    """evalutaes `lhs op rhs`, trying its best to return a string"""
    match (op):
        case "+":
            return strize_if_float(lhs + rhs)
        case "-":
            return strize_if_float(lhs - rhs)
        case "*":
            return strize_if_float(lhs * rhs)
        case "/":
            if (type(rhs) != pd.Series and rhs == 0): return float('nan')
            return strize_if_float(lhs / rhs)
        case "%":
            if (type(rhs) != pd.Series and rhs == 0): return float('nan')
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
    raise BeakscriptInterpretError(
        f"Parser error: Unexpected binary operator: {op}\n{BeakscriptInterpretError.curr_equation}\n{' ' * index + '^'}"
    )


def preproc_implicit_ops(tokens: list[Token]):
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
            and (
                isinstance(tokens[i - 1].symbol, pd.Series)
                or tokens[i - 1].symbol == ")"
            )
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


def rpn(tokens: list[Token]):
    """Converts `tokens` into a list of reverse polish notation tokens to make parsing much easier"""
    output = []
    ops: list[Token] = []

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


def solve_rpn(rpn_tokens: list[Token], df: pd.DataFrame):
    """parses `rpn_tokens`, using `df` to evaluate the headers"""
    stack_overflow = []  # the stack
    for t in rpn_tokens:
        tok = t.token
        sym = t.symbol

        if tok == TOKENS.HEADER:
            try:
                if "," in sym:  # multi-header = sum
                    stack_overflow.append(
                        df[[s.strip() for s in sym.split(",")]].sum(axis=1)
                    )
                else:
                    if "_" in sym or "?" in sym:
                        simr = re.compile(
                            "^" + re.escape(sym).replace("_", ".*").replace("\\?", ".+")
                        )
                        if isinstance(df, pd.DataFrame):
                            stack_overflow.append(
                                df.filter(regex=simr).iloc[0].reset_index(drop=True)
                            )
                        else:
                            stack_overflow.append(
                                df.filter(regex=simr).reset_index(drop=True)
                            )
                    else:
                        stack_overflow.append(df[sym])
            except KeyError:
                raise BeakscriptInterpretError(
                    f"Error at column {t.column} symbol '{sym}' ({tok}): header not present in dataframe\n{BeakscriptInterpretError.curr_equation}\n{' ' * (t.column - 1) + '^'}"
                ) from None
            continue

        if tok == TOKENS.LITERAL:
            stack_overflow.append(sym)
            continue

        if tok == TOKENS.UNARY_OP:
            if len(stack_overflow) < 1:
                raise BeakscriptInterpretError(
                    f"Error at column {t.column} symbol '{sym}' ({tok}): not enough operands\n{BeakscriptInterpretError.curr_equation}\n{' ' * (t.column - 1) + '^'}"
                )
            val = floatize_if_str(
                stack_overflow.pop()
            )  # pop closest value to apply unop to
            try:
                stack_overflow.append(evaluate_unary_operator(val, sym, t.column))
            except TypeError:
                raise BeakscriptInterpretError(
                    f"Error at column {t.column} symbol '{sym}' ({tok}): invalid operand type for unary operation: <operand: {type(val).__name__}>\n{BeakscriptInterpretError.equation}\n{' ' * (t.column - 1) + '^'}"
                ) from None
            continue

        if tok == TOKENS.BINARY_OP:
            if len(stack_overflow) < 2:
                raise BeakscriptInterpretError(
                    f"Error at column {t.column} symbol '{sym}' ({tok}): not enough operands\n{BeakscriptInterpretError.curr_equation}\n{' ' * (t.column - 1) + '^'}"
                )
            rhs = floatize_if_str(stack_overflow.pop())
            lhs = floatize_if_str(
                stack_overflow.pop()
            )  # pop operands (rhs first bc rpn notation) to apply biop to
            try:
                stack_overflow.append(evaluate_binary_operator(lhs, rhs, sym, t.column))
            except TypeError:
                raise BeakscriptInterpretError(
                    f"Error at column {t.column} symbol '{sym}' ({tok}): invalid operand types for binary operation: <lhs: {type(lhs).__name__}, rhs: {type(rhs).__name__}>\n{BeakscriptInterpretError.equation}\n{' ' * (t.column - 1) + '^'}"
                ) from None
            continue

    if len(stack_overflow) != 1:
        raise ValueError(f"Error: operation: {stack_overflow} cannot be simplified.")
    ret = stack_overflow[0]
    try:
        if (
            isinstance(ret, pd.Series) and ret.size == 1
        ):  # calling float on len 1 series will eventually throw an error, and we want to unwrap len 1 series.
            return float_or_int(ret.iloc[0])
        return float_or_int(ret)
    except:
        return ret


def eval_beakscript(equation: str, df: pd.DataFrame, equation_label=""):
    prog_ctr = 0
    prog_steps = ["Parsing", "Preprocessing", "RPN Parsing", "RPN Evaluation"]
    BeakscriptInterpretError.curr_equation = equation
    if not (equation_label.isspace() or equation_label == ""):
        BeakscriptInterpretError.curr_eq_name = equation_label
    try:
        tokens = parse_equation(equation, df)  # first parse the string
        prog_ctr += 1
        tokens = preproc_implicit_ops(tokens)  # then restructure the brackets
        prog_ctr += 1
        rpnResult = rpn(tokens)  # then convert the tokens to rpn
        prog_ctr += 1
        return solve_rpn(rpnResult, df)  # then evaluate it
    except Exception as e:
        if isinstance(e, BeakscriptInterpretError):
            raise
        tb = traceback.extract_tb(e.__traceback__)
        raise BeakscriptInterpretError(
            f"Unexpected error occured during step '{prog_steps[prog_ctr]}' ({prog_ctr + 1}/{len(prog_steps)}) of evaluation of equation\n{tb.format_frame_summary(tb[-1])}\n{type(e).__name__}: {e}"
        ) from None
