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
                "eqTokens": parse_equation(field["equation"])
            })
        for field in data["team-fields"]:
            config["teams"].append({
                "name": field["name"],
                "filters": [x for x in FILTERS if x in field],
                "deriveTok": parse_equation(field["derive"]),
            })
        for field in data["match-fields"]:
            config["matches"].append({
                "name": field["name"],
                "deriveTok": parse_equation(field["derive"]),
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

OPERATOR_STRINGS = [
    "+",
    "-",
    "*",
    "/",
    ">",
    "<",
    "=",
    "!",
    "}",
    "{"
]

COMP_STRINGS = [
    ">",
    "<",
    "}",
    "{",
    "=",
    "!"
]

PARENS = [
    "(",
    ")"
]

def parse_equation(equation: str):
    equation_tokens = []
    equation = equation.strip()
    i = 0
    while i < len(equation):
        c: str = equation[i]
        if c.isspace():
            i += 1
            continue
        if c in PARENS:
            equation_tokens.append(Token(TOKENS.PAREN, c))
        if c == "[" or c == "]":
            equation_tokens.append(Token(TOKENS.HEADER_COND, c))
        elif i == 0 and c == "-": # y = -thing
            equation_tokens.append(Token(TOKENS.UNARY_OP, "-"))
        elif c in OPERATOR_STRINGS:
            if equation[i - 1] in OPERATOR_STRINGS or equation[i - 1] == "(":
                equation_tokens.append(Token(TOKENS.UNARY_OP, c))
            else: equation_tokens.append(Token(TOKENS.BINARY_OP, c))
        elif c == "$":
            if (i < len(equation)): i += 1
            else: break
            ref_name = ""
            while (i < len(equation)) and (not equation[i] == "$") and (not equation[i] in ['$', '[', ']', *OPERATOR_STRINGS, *PARENS]):
                ref_name += equation[i]
                i += 1
            ref_name = ref_name.strip()
            equation_tokens.append(Token(TOKENS.HEADER, ref_name))
            continue
        else:
            literal_value = ""
            if (i >= len(equation)): break
            while (i < len(equation)) and (not equation[i] == "$") and (not equation[i] in ['$', '[', ']', *OPERATOR_STRINGS, *PARENS]):
                literal_value += equation[i]
                i += 1
            equation_tokens.append(Token(TOKENS.LITERAL, literal_value))
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

def eval_lex(tokens: List[Token], df: pd.DataFrame):
    # df refs
    i = 0
    while i < len(tokens):
        if (tokens[i].token == TOKENS.HEADER):
            if i + 1 < len(tokens) and tokens[i + 1].token == TOKENS.HEADER_COND:
                cols = tokens[i].symbol.split(",") if "," in tokens[i].symbol else tokens[i].symbol
                inner_exp = []
                b_index = i + 1
                bracket_count = 1
                i += 2
                while i < len(tokens):
                    if tokens[i].token == TOKENS.HEADER_COND:
                        if tokens[i].symbol == "[":
                            bracket_count += 1
                        else:
                            bracket_count -= 1
                            if bracket_count == 0: break
                    inner_exp.append(tokens[i])
                    i += 1
                mask = eval_lex(inner_exp, df)
                tokens[b_index - 1:i + 1] = [Token(TOKENS.LITERAL, df.loc[mask, cols] if type(cols) == str else df.loc[mask, cols].sum(axis=1))]
                i = b_index
            else:
                tokens[i] = Token(TOKENS.LITERAL, df[tokens[i].symbol.split(",")].sum(axis=1) if "," in tokens[i].symbol else df[tokens[i].symbol])
        i += 1

    i = 0
    # parens
    while i < len(tokens):
        if tokens[i].token == TOKENS.PAREN and tokens[i].symbol == "(":
            inner_exp = []
            p_index = i
            paren_count = 1
            i += 1
            while (i < len(tokens)):
                if tokens[i].token == TOKENS.PAREN:
                    paren_count += (1 if tokens[i].symbol == "(" else -1)
                    if paren_count == 0: break
                inner_exp.append(tokens[i])
                i += 1
            tokens[p_index:i + 1] = [Token(TOKENS.LITERAL, strize_if_float(eval_lex(inner_exp, df)))]
            i = p_index + 1
        else: i += 1

    i = 0
    # mul/div
    while i < len(tokens):
        if tokens[i].token == TOKENS.UNARY_OP and tokens[i].symbol == "-":
            tokens[i:i + 2] = [Token(TOKENS.LITERAL, str(-float(tokens[i + 1].symbol)))]
        elif tokens[i].token == TOKENS.BINARY_OP and (tokens[i].symbol == "*" or tokens[i].symbol == "/"):
            lhs = floatize_if_str(tokens[i - 1].symbol)
            rhs = floatize_if_str(tokens[i + 1].symbol)
            tokens[i - 1:i + 2] = [Token(TOKENS.LITERAL, strize_if_float(lhs * rhs if tokens[i].symbol == "*" else strize_if_float(lhs / rhs)))]
            i -= 1
        i += 1

    i = 0
    # add/sub
    while i < len(tokens):
        if tokens[i].token == TOKENS.BINARY_OP and (tokens[i].symbol == "+" or tokens[i].symbol == "-"):
            lhs = floatize_if_str(tokens[i - 1].symbol)
            rhs = floatize_if_str(tokens[i + 1].symbol)
            tokens[i - 1:i + 2] = [Token(TOKENS.LITERAL, strize_if_float(lhs + rhs) if tokens[i].symbol == "+" else strize_if_float(lhs - rhs))]
            i -= 1
        i += 1

    i = 0
    # boolean ops
    while i < len(tokens):
        if tokens[i].token == TOKENS.BINARY_OP and (tokens[i].symbol in COMP_STRINGS):
            lhs = floatize_if_str(tokens[i - 1].symbol)
            rhs = floatize_if_str(tokens[i + 1].symbol)
            match (tokens[i].symbol):
                case ">":
                    value = strfloatize_if_bool(lhs > rhs)
                case "<":
                    value = strfloatize_if_bool(lhs < rhs)
                case "}":
                    value = strfloatize_if_bool(lhs >= rhs)
                case "{":
                    value = strfloatize_if_bool(lhs <= rhs)
                case "=":
                    value = strfloatize_if_bool(lhs == rhs)
                case "!":
                    value = strfloatize_if_bool(lhs != rhs)
            tokens[i - 1:i + 2] = [Token(TOKENS.LITERAL, value)]
            i -= 1
        i += 1

    return floatize_if_str(tokens[0].symbol)

if __name__ == "__main__":
    print(eval_lex(parse_equation("2 + 3 + $TN,MN[$TC = B]"), pd.DataFrame({
        "TN": [3, 5],
        "MN": [2, 3],
        "TC": ["R", "B"]
    })))