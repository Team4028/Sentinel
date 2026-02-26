from pathlib import Path
import unittest
import pandas as pd
import json
from jsonschema import Draft7Validator
from pandas.testing import assert_series_equal
import yaml
try:
    from lib.data_config import eval_beakscript, UnpackList
except ModuleNotFoundError:
    from src.lib.data_config import eval_beakscript, UnpackList


class TestBeakscript(unittest.TestCase):
    """One must imagine `OK`"""

    def test_regex_header(self):
        """Test for wildcard-based headers"""
        assert_series_equal(
            eval_beakscript(
                "$_A",
                pd.DataFrame({"A": [0], "1A": [1], "2A": [2], "3B": [3]}),
                "Unittest Regex",
            ),
            pd.Series([0, 1, 2]),
            check_names=False,
        )
        assert_series_equal(
            eval_beakscript(
                "$?A",
                pd.DataFrame({"A": [0], "1A": [1], "2A": [2], "3B": [3]}),
                "Unittest Regex",
            ),
            pd.Series([1, 2]),
            check_names=False,
        )

    def test_header_sum(self):
        """Test the implicit $A,B header summation"""
        self.assertEqual(
            eval_beakscript(
                "$A,B", pd.DataFrame({"A": [1], "B": [2]}), "Unittest Header Sum"
            ),
            3,
        )

    def test_type_coercion_and_types(self):
        """Test various automatic type conversions"""
        self.assertEqual(eval_beakscript("'20' == 20", {}, "Unittest quotes"), 0)
        self.assertIsInstance(eval_beakscript("20.5", {}, "Unittest float"), float)
        self.assertIsInstance(eval_beakscript("20", {}, "Unittest int"), int)
        self.assertIsInstance(
            eval_beakscript("*{1, 2, 3}", {}, "Unittest UnpackList"), UnpackList
        )

    def test_unary_operators(self):
        """Test operators with 1 argument"""
        assert_series_equal(
            eval_beakscript("{1, *{2, 3}, 4}", {}, "Unittest U*"),
            pd.Series([1, 2, 3, 4]),
        )
        self.assertEqual(eval_beakscript("-5", {}, "Unittest U-"), -5)
        self.assertEqual(eval_beakscript("!1", {}, "Unittest U!"), 0)
        self.assertEqual(
            eval_beakscript("@avg{1, 2, 6, 4}", {}, "Unittest U@avg"), 3.25
        )
        self.assertEqual(eval_beakscript("@max{1, 2, 6, 4}", {}, "Unittest U@max"), 6)
        self.assertEqual(eval_beakscript("@min{1, 2, 6, 4}", {}, "Unittest U@min"), 1)
        self.assertEqual(eval_beakscript("@sum{1, 2, 6, 4}", {}, "Unittest U@sum"), 13)
        self.assertEqual(eval_beakscript("@len{1, 2, 6, 4}", {}, "Unittest U@len"), 4)

    def test_binary_operators(self):
        """Test operators with 2 arguments"""
        self.assertEqual(
            eval_beakscript(
                "$A[$B == b]",
                pd.DataFrame({"A": [1, 2, 3], "B": ["a", "b", "c"]}),
                "Unittest B[]",
            ),
            2,
        )
        self.assertEqual(eval_beakscript("5 * 2", {}, "Unittest B* 1"), 10)
        assert_series_equal(
            eval_beakscript("{3, 2} * {2, 1}", {}, "Unittest B* 2"), pd.Series([6, 2])
        )
        assert_series_equal(
            eval_beakscript("{3, 2} * 5", {}, "Unittest B* 3"), pd.Series([15, 10])
        )
        self.assertEqual(eval_beakscript("5 / 2", {}, "Unittest B/ 1"), 2.5)
        assert_series_equal(
            eval_beakscript("{3, 2} / {2, 1}", {}, "Unittest B/ 2"),
            pd.Series([1.5, 2.0]),
        )
        assert_series_equal(
            eval_beakscript("{3, 2} / 5", {}, "Unittest B/ 3"), pd.Series([0.6, 0.4])
        )
        self.assertEqual(eval_beakscript("5 % 2", {}, "Unittest B% 1"), 1)
        assert_series_equal(
            eval_beakscript("{3, 2} % {2, 1}", {}, "Unittest B% 2"), pd.Series([1, 0])
        )
        assert_series_equal(
            eval_beakscript("{3, 2} % 5", {}, "Unittest B% 3"), pd.Series([3, 2])
        )
        self.assertEqual(eval_beakscript("5 + 2", {}, "Unittest B+ 1"), 7)
        assert_series_equal(
            eval_beakscript("{3, 2} + {2, 1}", {}, "Unittest B+ 2"), pd.Series([5, 3])
        )
        assert_series_equal(
            eval_beakscript("{3, 2} + 5", {}, "Unittest B+ 3"), pd.Series([8, 7])
        )
        self.assertEqual(eval_beakscript("5 - 2", {}, "Unittest B- 1"), 3)
        assert_series_equal(
            eval_beakscript("{3, 2} - {2, 1}", {}, "Unittest B- 2"), pd.Series([1, 1])
        )
        assert_series_equal(
            eval_beakscript("{3, 2} - 5", {}, "Unittest B- 2"), pd.Series([-2, -3])
        )
        self.assertEqual(eval_beakscript("a ` pad", {}, "Unittest B` 1"), 1)
        self.assertEqual(eval_beakscript("a ` pod", {}, "Unittest B` 2"), 0)
        self.assertEqual(eval_beakscript("1 > 0", {}, "Unittest B> 1"), 1)
        self.assertEqual(eval_beakscript("1 > 2", {}, "Unittest B> 2"), 0)
        self.assertEqual(eval_beakscript("1 < 0", {}, "Unittest B< 1"), 0)
        self.assertEqual(eval_beakscript("1 < 2", {}, "Unittest B< 2"), 1)
        self.assertEqual(eval_beakscript("1 >= 1", {}, "Unittest B>= 1"), 1)
        self.assertEqual(eval_beakscript("1 >= 2", {}, "Unittest B>= 2"), 0)
        self.assertEqual(eval_beakscript("1 <= 1", {}, "Unittest B<= 1"), 1)
        self.assertEqual(eval_beakscript("1 <= 0", {}, "Unittest B<= 2"), 0)
        self.assertEqual(eval_beakscript("1 == 1", {}, "Unittest B== 1"), 1)
        self.assertEqual(eval_beakscript("1 == 0", {}, "Unittest B== 2"), 0)
        self.assertEqual(eval_beakscript("1 != 1", {}, "Unittest B!= 1"), 0)
        self.assertEqual(eval_beakscript("1 != 0", {}, "Unittest B!= 2"), 1)
        self.assertEqual(eval_beakscript("1 ^ 1", {}, "Unittest B^ 1"), 0)
        self.assertEqual(eval_beakscript("1 ^ 0", {}, "Unittest B^ 2"), 1)
        self.assertEqual(eval_beakscript("0 ^ 1", {}, "Unittest B^ 3"), 1)
        self.assertEqual(eval_beakscript("0 ^ 0", {}, "Unittest B^ 4"), 0)
        self.assertEqual(eval_beakscript("1 & 1", {}, "Unittest B& 1"), 1)
        self.assertEqual(eval_beakscript("1 & 0", {}, "Unittest B& 2"), 0)
        self.assertEqual(eval_beakscript("0 & 1", {}, "Unittest B& 3"), 0)
        self.assertEqual(eval_beakscript("0 & 0", {}, "Unittest B& 4"), 0)
        self.assertEqual(eval_beakscript("1 | 1", {}, "Unittest B| 1"), 1)
        self.assertEqual(eval_beakscript("1 | 0", {}, "Unittest B| 2"), 1)
        self.assertEqual(eval_beakscript("0 | 1", {}, "Unittest B| 3"), 1)
        self.assertEqual(eval_beakscript("0 | 0", {}, "Unittest B| 4"), 0)

    def test_operator_prec(self):
        """Test that operators are evaluated in the right order"""
        self.assertEqual(eval_beakscript("0 | 1 & 0", {}, "Unittest OoO 1"), 0)
        self.assertEqual(
            eval_beakscript("5 / 2 * 2", {}, "Unittest OoO 2"), 5
        )  # test L->R for equal precedence
        self.assertEqual(eval_beakscript("5 + 3 * 2", {}, "Unittest OoO 3"), 11)
        self.assertEqual(eval_beakscript("3 * (5 + 2)", {}, "Unittest OoO 4"), 21)
        self.assertEqual(eval_beakscript("0 & 1 == 0", {}, "Unittest OoO 5"), 0)
        self.assertEqual(eval_beakscript("0 & 1 != 1", {}, "Unittest OoO 6"), 0)
        self.assertEqual(eval_beakscript("-2 + 5", {}, "Unittest OoO 7"), 3)
        self.assertEqual(
            eval_beakscript(
                "($A * $B)[frank ` $C]",
                pd.DataFrame(
                    {
                        "A": [3, 2, 1],
                        "B": [5, 2, 8],
                        "C": ["frankestein", "modern", "prometheus"],
                    }
                ),
                "Unittest OoO 8",
            ),
            15,
        )
        assert_series_equal(
            eval_beakscript("@lenn-{2, 3, 5, 6}", {}, "Unittest OoO 9"),
            pd.Series([-1, -2, -4, -5]),
        )

    def test_config_files_scheme(self):
        """Ensure that the configuration files match the schema"""
        with open("./config/schema.json", "r") as f:
            schema = json.load(f)
        for file in Path("./config").glob("field-config-*.yaml"):
            with file.open("r") as f:
                data = yaml.safe_load(f)
            validator = Draft7Validator(schema)
            errors = sorted(validator.iter_errors(data), key=lambda e: e.path)
            if errors:
                messages = [f"{list(e.path)}: {e.message}" for e in errors]
                self.fail(
                    f"Schema validation failed in file {file}:\n{'\n'.join(messages)}"
                )