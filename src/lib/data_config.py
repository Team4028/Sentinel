import yaml
from enum import Enum
from typing import List
import pandas as pd

def read_config():
    try:
        with open("config/field-config.yaml") as f:
            data = yaml.safe_load(f)
        return data
    except FileNotFoundError:
        print("Error: File not found at path")
    except yaml.YAMLError as e:
        print(f"YAML Parser encountered an error: {e}")


FILTERS = [
    "avg",
    "max",
    "fil"
]

def lex_config():
    data = read_config()
    config = {
        "compute": [],
        "teams": [],
        "matches": [],
        "predict-metric": "",
        "deep-predict": [],
    }
    if data:
        for field in data['compute-fields']:
            config["compute"].append({
                "name": field["name"],
                "eq": field["equation"]
            })
        for field in data["team-fields"]:
            config["teams"].append({
                "name": field["name"],
                "filters": [x for x in FILTERS if x in field],
                "derive": field["derive"],
            })
        for field in data["match-fields"]:
            config["matches"].append({
                "name": field["name"],
                "derive": field["derive"],
                "filters": [x for x in FILTERS if x in field]
            })
        config["p-metric"] = data["predict-metric"]
        for field in data["depth-predict-fields"]:
            config["deep-predict"].append({
                "name": field["name"],
                "source": field["source"]
            })
    return config

# BEAKSCRIPT
class TOKENS(Enum):
    BINARY_OP = 0
    UNARY_OP = 1
    LITERAL = 2
    HEADER = 3
    HEADER_COND = 4
    PAREN = 5
    LIST_LITERAL = 6

class Token:
    token: TOKENS
    symbol: str | pd.DataFrame
    def __init__(self, token, symbol):
        self.token = token
        self.symbol = symbol

    def __str__(self):
        return f"{self.token.name}: {self.symbol}"
    
    def __repr__(self):
        return f"{self.token.name}: {self.symbol}"

UNOPS = [
    "-",
    "~"
    "@avg",
    "@max",
    "@min",
    "@sum",
    "@len",
]

OPERATOR_STRINGS = [
    "+",
    "-",
    "*",
    "/",
    "%",
    ">",
    "<",
    "=",
    "!",
    "&",
    "|",
    "~"
]

COMP_EXTEND = [
    ">",
    "<",
]

PARENS = [
    "(",
    ")"
]

LIST_LITERALS = [
    "{",
    "}"
]

UNOP_PRECEDENCE = {
    "-": 7,
    "~": 7,
    "@sum": 7,
    "@avg": 7,
    "@max": 7,
    "@min": 7,
    "@len": 7,
}

BIOP_PRECEDENCE = {
    "[]": 10,
    "*": 6,
    "/": 6,
    "%": 6,
    "+": 5,
    "-": 5,
    ">": 4,
    "<": 4,
    ">=": 4,
    "<=": 4,
    "!": 3,
    "=": 3,
    "&": 2,
    "|": 1,
}

def check_last_nowhitespace(s, i):
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
        if c.isspace():
            i += 1
            continue
        if c == "@":
            if i + 3 < len(equation):
                equation_tokens.append(Token(TOKENS.UNARY_OP, "".join(equation[i + j] for j in range(4))))
                i += 4
            else: i += 1
            continue
        if c in PARENS:
            equation_tokens.append(Token(TOKENS.PAREN, c))
        elif c == "[" or c == "]":
            equation_tokens.append(Token(TOKENS.HEADER_COND, c))
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
                elif c == "," and b_count == 1 and p_count == 0:
                    list_tokens.append(curr.strip())
                    curr = ""
                else:
                    curr += c
                i += 1
            literal_list = [eval_beakscript(item, df) for item in list_tokens]
            equation_tokens.append(Token(TOKENS.LITERAL, pd.Series(literal_list)))
            
        elif c in OPERATOR_STRINGS:
            if i == 0 or check_last_nowhitespace(equation, i) in OPERATOR_STRINGS or equation[i - 1] in ["(", "[", "{"] or (i - 4 >= 0 and equation[i - 4] == "@"):
                equation_tokens.append(Token(TOKENS.UNARY_OP, c))
            elif equation[i] in COMP_EXTEND:
                if i + 1 < len(equation) and equation[i + 1] == "=":
                    equation_tokens.append(Token(TOKENS.BINARY_OP, c + "="))
                    i += 1
                else:
                    equation_tokens.append(Token(TOKENS.BINARY_OP, c))
            else: equation_tokens.append(Token(TOKENS.BINARY_OP, c))
        elif c == "$":
            if (i < len(equation)): i += 1
            else: break
            ref_name = ""
            while (i < len(equation)) and (not equation[i] == "$") and (not equation[i] in ['$', '[', ']', '@', *OPERATOR_STRINGS, *PARENS, *LIST_LITERALS]):
                ref_name += equation[i]
                i += 1
            equation_tokens.append(Token(TOKENS.HEADER, ref_name.strip()))
            continue
        else:
            literal_value = ""
            if (i >= len(equation)): break
            while (i < len(equation)) and (not equation[i] == "$") and (not equation[i] in ['$', '[', ']', '@', *OPERATOR_STRINGS, *PARENS, *LIST_LITERALS]):
                literal_value += equation[i]
                i += 1
            equation_tokens.append(Token(TOKENS.LITERAL, literal_value.strip()))
            continue
        i += 1
    return equation_tokens

def floatize_if_str(x):
    try:
        return float(x) if type(x) == str else x
    except ValueError:
        return x
def strize_if_float(x):
    try:
        return str(x) if type(x) == float else x
    except ValueError:
        return x
    
def strfloatize_if_bool(x):
    if type(x) == bool:
        return "1" if x else "0"
    return x

def df_safe_and(a, b):
    try:
        return a & b
    except:
        return bool(a) and bool(b)
    
def df_safe_or(a, b):
    try:
        return a | b
    except:
        return bool(a) or bool(b)
    
def evaluate_unary_operator(x, op):
    match (op):
        case "-":
            return strize_if_float(-x)
        case "~":
            if type(x) == pd.Series:
                return ~x
            return strfloatize_if_bool(not x)
        case "@avg":
            try:
                return strize_if_float(x.mean()) # pd
            except:
                try:
                    return strize_if_float(sum(x) / len(x)) # list
                except:
                    return strize_if_float(x) # else
        case "@max":
            try:
                return strize_if_float(x.max()) # pd
            except:
                try:
                    return strize_if_float(max(x)) # list
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
                except: return strize_if_float(x)
        case "@len":
            try:
                return strize_if_float(len(x))
            except: strize_if_float(x)
    print(f"Parser Error: Unknown unary operation: {op}")
    return "nan"

    
def evaluate_binary_operator(lhs, rhs, op):
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
        case ">":
            return strfloatize_if_bool(lhs > rhs)
        case "<":
            return strfloatize_if_bool(lhs < rhs)
        case ">=":
            return strfloatize_if_bool(lhs >= rhs)
        case "<=":
            return strfloatize_if_bool(lhs <= rhs)
        case "=":
            return strfloatize_if_bool(lhs == rhs)
        case "!":
            return strfloatize_if_bool(not (lhs == rhs))
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

            if i + 1 < len(tokens) and tokens[i + 1].token == TOKENS.HEADER_COND and tokens[i + 1].symbol == "[":
                i += 2
                inner = []
                b_count = 1
                while i < len(tokens):
                    if tokens[i].token == TOKENS.HEADER_COND:
                        if tokens[i].symbol == "[":
                            b_count += 1
                        else:
                            b_count -= 1
                            if b_count == 0: break
                    inner.append(tokens[i])
                    i += 1
                output.append(Token(TOKENS.BINARY_OP, "[]"))
                output.append(Token(TOKENS.PAREN, "("))
                output.extend(preproc_implicit_ops(inner))
                output.append(Token(TOKENS.PAREN, ")"))
        elif t.token == TOKENS.HEADER_COND and i > 0 and (type(tokens[i - 1].symbol) == pd.Series or tokens[i - 1].symbol == ")"):
            i += 1
            inner = []
            b_count = 1
            while i < len(tokens):
                if tokens[i].token == TOKENS.HEADER_COND:
                    if tokens[i].symbol == "[":
                        b_count += 1
                    else:
                        b_count -= 1
                        if b_count == 0: break
                inner.append(tokens[i])
                i += 1
            output.append(Token(TOKENS.BINARY_OP, "[]"))
            output.append(Token(TOKENS.PAREN, "("))
            output.extend(preproc_implicit_ops(inner))
            output.append(Token(TOKENS.PAREN, ")"))
        else:
            output.append(t)

        i += 1
    return output

def rpn(tokens: List[Token]):
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
            while ops and ops[-1].token in [TOKENS.UNARY_OP, TOKENS.BINARY_OP] and \
                (((UNOP_PRECEDENCE if ops[-1].token == TOKENS.UNARY_OP else BIOP_PRECEDENCE)[ops[-1].symbol] >= prec_list[token.symbol]) if l_assoc else ((UNOP_PRECEDENCE if ops[-1].token == TOKENS.UNARY_OP else BIOP_PRECEDENCE)[ops[-1].symbol] > prec_list[token.symbol])):
                output.append(ops.pop())
            ops.append(token)
        elif token.token == TOKENS.PAREN and token.symbol:
            if token.symbol == "(":
                ops.append(Token(TOKENS.PAREN, "("))
            else:
                while ops and not (ops[-1].token == TOKENS.PAREN and ops[-1].symbol == "("):
                    output.append(ops.pop())
                ops.pop()
        elif token.token == TOKENS.HEADER or token.token == TOKENS.HEADER_COND:
            output.append(token)

    while ops:
        output.append(ops.pop())

    return list(output)

def solve_rpn(rpn_tokens: List[Token], df: pd.DataFrame):
    stack_overflow = []
    for t in rpn_tokens:
        tok = t.token
        sym = t.symbol

        if tok == TOKENS.HEADER:
            if "," in sym:
                stack_overflow.append(df[[s.strip() for s in sym.split(",")]].sum(axis=1))
            else:
                stack_overflow.append(df[sym])
            continue

        if tok == TOKENS.LITERAL:
            stack_overflow.append(sym)
            continue

        if tok == TOKENS.UNARY_OP:
            val = floatize_if_str(stack_overflow.pop())
            stack_overflow.append(evaluate_unary_operator(val, sym))
            continue

        if tok == TOKENS.BINARY_OP:
            rhs = floatize_if_str(stack_overflow.pop())
            lhs = floatize_if_str(stack_overflow.pop())
            stack_overflow.append(evaluate_binary_operator(lhs, rhs, sym))
            continue

    if len(stack_overflow) != 1:
        raise ValueError(f"Error: operation: {stack_overflow} cannot be simplified.")
    ret = stack_overflow[0]
    try:
        return float(ret)
    except:
        return ret

def eval_beakscript(equation: str, df: pd.DataFrame):
    tokens = parse_equation(equation, df)
    tokens = preproc_implicit_ops(tokens)
    rpnResult = rpn(tokens)
    return solve_rpn(rpnResult, df)

if __name__ == "__main__":
    # test beakscript
    print(eval_beakscript("@sum$TN,MN[$TC = R | $TC = Y]", pd.DataFrame({
        "TN": [3, 5, 2],
        "MN": [2, 3, 8],
        "TC": ["R", "B", "Y"]
    })))

    print(eval_beakscript("{($TN,MN), $TN - $MN}", pd.DataFrame({
        "TN": [3, 5, 2],
        "MN": [2, 3, 8],
        "TC": ["R", "B", "Y"]
    })))

    print(eval_beakscript("@len{2, 3, 5, 6}", {}))
    print(eval_beakscript("@lenn-{2, 3, 5, 6}", {})) # had to keep this one around because I was impressed that it worked