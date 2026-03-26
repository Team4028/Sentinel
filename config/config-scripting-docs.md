# Sentinel Config Pseudolang/Formula Interpreter (Working Title: Beakscript) Documentation

⚠️ In the below documentation, I may use header/column/field interchangably. In this case, their meaning is identical to represent a column labelled via a header in the csv data. The deviation stems from the fact that the label is called a header, the labelled column is called a column (believe it or not), and in the grafana frontend, these labelled columns are called fields. Also lists and series will occasionally be referred to interchangebly. In almost all cases, both terms refer to the `pandas.Series` data type representing a list of data points with optional header names ⚠️

<details>
<summary> <h2> Recency </h2> </summary>

Docs up to date?
- [x] FOR NOW
- [ ] NO

</details>
<details>
<summary> <h2> Basic Operations </h2></summary>

Basic operations are supported, identical to common programming languages and mathematics
- In the below notation, (=>) is used to denote the result of the expression, and is not part of the expression itself
- Below, the operations for addition (+), subtraction (-), multiplication (*), division (/), and modulo (%) are represented
```
5 + 5 (=> 10)
4 - 6 (=> -2)
3 * 2 (=> 6)
2 / 5 (=> 0.4)
5 % 3 (=> 2)
```
</details>

<details>
<summary> <h2> References </h2> </summary>

You can reference a column/header/field of the input data using the `$` operator. For example, if I want to obtain the number of L4 coral cycles scored in
auton (listed in the scouting data's csv as 'TL4'), I can use the expression `$TL4` to reference the field. Additionally, fields can be implicitly combined via element-wise summation via a comma: Referencing `$TL4,TL3` will yield a list that is the sum of all the values of TL4 and TL3, meaning that if TL4 was the list {3, 2} and TL3 was the list {4, 5}, `$TL4,TL3` would yield the list {7, 7}. This notation DOES NOT work inside of a list literal, due to the fact that commas distinguish elements. This limitation can be circumvented via the use of parentheses (ie. `{($AL4,TL4)}` = `{$AL4 + $TL4}`, `{$AL4,$TL4}` ≠ `{$AL4 + $TL4}`)

- As these expressions are applied to the whole data row by row, headers are effectively equivelent to the entire column of data, and operations can thus be performed on them to derive new fields. In the compute section, for example, you can define new fields to append to your raw data csv. For example, you could add a new field for total cycles with the equation
`$AL1,AL2,AL3,AL4,TL1,TL2,TL3,TL4,ATP,AP,ATB,AB`, which would create a new list that is the sum of all of the constituent fields.
- Because both references and literals are not delimited by spaces, they are space-safe, and cannot be seperated by only a space. `$TN foo bar` will look for the field "TN foo bar" in the data. HOWEVER, fields/literals ARE trimmed, so `$TN        + 1` is the same as `$TN+1` and will NOT look for the field "TN&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"  
### Wildcards
Additionally, the `_` character can be used as a wildcard to match multiple headers (see probably still present use in [field-config-2026_subjective.yaml](./field-config-2026_subjective.yaml) in the preproccessing section [edit: referenced script now uses the similar `?` operator below]). For example, if the headers in the data are `TeamA`, `TeamB`, `ScoreA`, and `ScoreB`, the expression `$_A` will result in a series containing `TeamA` amd `ScoreA`, whereas `$Team_` will be a series containing `TeamA` and `TeamB`. The `_` is equivalent to the python regex `.*` expression, which matches *0 or more* characters, meaning `$_A` will also match the header `A`.
- The `?` character is another wildcard operator. It translates to the python regex `.+`, and so it matches *1 or more* wildcard characters, meaning that `$?A` will NOT match the header `A`, but WILL match the header `TeamA`. 
</details>

<details>
<summary> <h2> Filtration </h2> </summary>

You can filter lists with conditionals via the `[]` operator. To do this, you can append [<condition>] following a list. For example, to filter the drive rating (DR) list to matches where played defense (D) is true, represented by 1, you can use the expresion `$DR[$D = 1]`, where `=` is tests for equivalence. This works for strings too. For example, the expression `$MN[$TN = 422 & $TC = R1]` represents the list of all match numbers where team 422 is Red 1.
### Conditionals and conditional operators
Conditionals are represented internally by two different types. For list filtration like `$A[$B = x]`, A is indexed with a series of booleans acting as a mask. For normal operations like `3 = 5`, the condition is condensed to a float, with True => 1.0 and False => 0.0. This means that `(2 == 2) + (3 > 2)` is equal to 2, and the above `3 = 5` equals 0. 
The conditional operators avaliable in Beakscript are `=` equals (can also use `==`), `!` not equals (can also use `!=`), `>` greater than, `<` less than, `>=` greater or equal, and `<=` less or equal. The `` ` `` operator can be used to check if a field contains a literal (ie. check if alliance is red: `` R ` $TeamColor ``). Additionally, the `^` xor,  `&` and, and `|` or operators can be used to combine expressions, and the boolean NOT operator, represented by `!` (`!` is also not equals when used in a binary context), can be used to negate boolean expressions (`!(5 > 3)` equals 0.0, which represents False)

</details>

<details>
<summary> <h2> Lists </h2> </summary>

Apart from using references to make lists (`$A`), you can also manually define lists. This can be useful for packaging operations together. For example, to sum up the total score from algae in a match, you can use the expression `@sum({($ATP,AP), ($ATB,AB)} * {2, 4})`. Because each proceser algae is worth 2 and barge is 4, this expression first adds the total proccessor and barge cycles together with the compound header notation `$A,B`, using parenthesis to avoid instead specifying a list `{$A, B}` where B is a literal because it's a seperate list entry, and then it multiplies the list `{($ATP, AP), ($ATB,AB)}` with the list `{2, 4}`, which turns the list from cycles to score. Finally, the expression uses the unary list operator `@sum` to add together all of the elements in the resultant list, which sums up the score contributions from processor and barge algae.
### Unpacking
Lists can also be 'unpacked' via the `*` unary operator when nested in another list. Similar to python, this allows you to pass a list in as indicies of another list as opposed to a single nested element. For example, in the list `{1, 2, {3, 4}, 5}`, the third element of the list is another list containing 3 and 4, but the expression `{1, 2, *{3, 4}, 5}` is equivalent to `{1, 2, 3, 4, 5}`, unpacking the nested list into part of the parent. This is useful for applications such as reference wildcards (above), where the expression `$_A_` evaluates to a series of all of the headers with `A` in them, and the list can be unpacked (`*$_A_`) to fit nicely into another list (ie. `{*$_A_, *$_B_}` for a list containing all of the headers with `A` or `B`). An application of this can be seen (as of 16 feb 2026) in the aforementioned preprocessing section of [field-config-2026_subjective.yaml](./field-config-2026_subjective.yaml). Use of the unpack operator ouside of list nesting (ie. `*{1, 2}` is not nested) converts the series into an actual `list` object, which under most circumstances will have no effect on the process, but will probably mess up filtration and may break some element-wise operations, so it is generally safest to keep the use of this operator confined to the intended use-case.

### List Unary Operators
- `*`, unpack (see above section)
- `@sum`, adds up all the elements in a list, collapsing it into a single number (`@sum{2, 3}` equals 5)
- `@avg`, returns the average of the list, so `@avg{2, 3}` equals 2.5
- `@min`, returns the minimum value of the list, so `@min{2, 3}` equals 2 
- `@max`, returns the maximum value of the list, so `@max{2, 3}` equals 3
- `@len`, returns the length of the list, so `@len{2, 3}` equals 2

</details>

<details>
<summary> <h2> Operator Precedence </h2> </summary>

Operator precedence defines the order in which operators are evalutated (order of operations). For example 2 + 2 * 3 is 8, not 12, because the multiplication is carried out before the addition. The order of evaluation of operators is as follows (left to right for operators of equal precedence):

- `[]`: this operation is carried out first, but the condition within it is fully evaluated before it is applied
- `- and ! and * and @sum and @avg and @min and @max and @len`: all unary operators come next, evaluated from left to right 
- `* and / and %`: next is mult/div/mod operators, which are mathematically first by PEMDAS
- `+ and -`: next, add/sub (by PEMDAS)
- ``> and < and >= and <= and ` ``: gt/lt/ge/le/in is next
- `=(=) and !(=)`: following the c++ standard, equal and not equal come after >, <, etc.
- `& and ^`: next are AND and XOR, which are before OR by c++ standard
- `|`: finally, OR is the last operator evaluated

It is also important to note that parenthesis can be used to short-circut order of operations, even on the `[]` operator (ex. `($TN * $MN)[$TC = R1]`)

</details>

<details>
<summary> <h2> Configuration Sections </h2> </summary>

This part is a little hard to explain so it's probably best to take a look at the following examples
- [2025](./field-config-2025.yaml)
- [2026 objective](./field-config-2026_objective.yaml)
- [2026 subjective](./field-config-2026_subjective.yaml) (wip)
<br>

By order as defined in the schema:
### Headers
Defines names for the headers for the *INPUT* csv file. This is what you will be referencing with future processing in scripts
### Preprocessing operations
This allows you some room to restructure the data. The `operation` field should output a series or series/list of series, and can be used to restructure or redefine the rows of the data. For example, [field-config-2026_subjective.yaml](./field-config-2026_subjective.yaml) uses the preproc section to transform the row structure [MN, AC, SI, T1, \<T1 DATAS>, T2, \<T2 DATAS>, T3, \<T3 DATAS>, EID] into [MN, AC, SI, TN \<T DATAS>, EID] for each team to better fit the processing structure after getting the former structure from QRScout
### Filter Unique fields
This section allows you to define the fields by which duplicate matches are detected (which fields being identical are indicitive of a duplicate). Generally, this should be something like match number + team number, but depending on the data flow you may want it differently.<br>
The headers you want to use to detect duplicates should be passed in as a string array.
### Subjective SVD fields
This section defines a series of new fields to generate using SVD decomposition based on source comparison fields for subjective relative scouting (see [pairwise docs](https://www.pairwisetool.com/help_svd)). The `name` field will name the output header of the process. `source` denotes the field to source the difference in ranking of the two teams from (no `$` before header name). `compare-team-source` defines the header under which the team being compared to the current team in the `source` field is denoted. Adding `variance-score` will output a new field (`name` + ' Variance') giving the variance score (lower = more consistent). Adding `stability` appends a field (`name` + ' Stability') that denotes the confidence (greater number => better).
### Compute Fields
This section defines new row-wise fields to add to the base output csv. This section is generally used for intermediary calculations like total score in a match by a team, total cycles, anything that matters to be calculated and then averaged out for a team. The `name` property defines the header name of the new field, and the `equation` is what defines the calculation (as a beakscript string). Calculations are done top to bottom through the list of fields, and so lower equations can reference previously defined compute fields (ie. a list of compute fields has the first one calculating the total cycles by that team in that match, and the next field multiplies the former by the score per game piece to get the total score).
### Team Fields
This section defines what data is collected for each team. The `derive` field defines the beakscript string that is used to derive a given field from the output data (a combination of the inputted data and the `Compute Fields` above), and `avg`, `max`, and `fil` fields can be added to automatically add `Average <field name>`, `Max <field name>`, and `Filtered <field name>` additional fields which replace the original field (where filtered is an average of the data set after applying MAD filtering with += 2 MAD from the median).
### Match Fields
These fields are data for each match, primarily used for the match view section of the dashboard. `static` can be used to process based on all 6 teams in a match, where the `derive` either outputs a list to assign to the 6 teams or a single value that's the same for all of them, otherwise the field generally cooresponds to a team's performance in a match.
### Predict Metric
This field simply denotes the field to use for scoring predictions (usually average score, filtered score, or similar). The `source` property is solely the *NAME* of the field to use, so there should be no leading `$`
### Depth Predict Fields
This section denotes additional statistics to show in the match preview section (ie. climb capabilities, average cycles, etc.). Once again, these stats are specified via a `source` field that takes in the *NAME* (no `$`) of the desired field.
### Data Tests
This section denotes tests that the code can perform to ensure all of the input data 'looks good'. While listed last here, these tests are performed directly after preprocessing, so they can not reference any compute fields or similar. The test's `expression` should return a boolean or 1/0 for true/false respectively. Below are some standard tests as an example:
```yaml
- name: Team/Match Number greater than zero
  expression: $TN > 0 & $MN > 0
- name: Team Color is valid
  expression: B ` $TC | R ` $TC
- name: Verify year
  expression: "'2026' ` $EID" # use quotes because otherwise 2026 is coerced into a number and number in string will always be false
```

</details>

<details>
<summary> <h2> Using the schema (in VSCode) </h2> </summary>

- Open Settings from File->Preferences->Settings or with Ctrl+, (Cmd+, for mac)
- Search for `yaml.schemas`, click edit in settings.json
- Add the following line in the yaml.schemas object in settings.json:
```json
// probably other stuff
"yaml.schemas":  {
    // possibly other stuff
    "./config/schema.json": "config/*.yaml" // Add this line
},
// probably other stuff
```
</details>

<details>
<summary> <h2> Testing </h2> </summary>

There are a number of unit tests defined for beakscript in the `TestBeakscript` class.
These can be run via `python src/tests/libtest.py` (or without `-v`) or via unittest with `python -m unittest src.tests.libtest.TestBeakscript -v` (or no `-v`) -- `-v` is just for verbose, which actually prints out the tests being run


</details>
