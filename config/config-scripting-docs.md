# Sentinel Config Pseudolang/Formula Interpreter (Working Title: Beakscript) Documentation

⚠️ In the below documentation, I may use header/column/field interchangably. In this case, their meaning is identical to represent a column labelled via a header in the csv data. The deviation stems from the fact that the label is called a header, the labelled column is called a column (believe it or not), and in the grafana frontend, these labelled columns are called fields. ⚠️

## Basic Operations
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

## References
You can reference a column/header/field of the input data using the `$` operator. For example, if I want to obtain the number of L4 coral cycles scored in
auton (listed in the scouting data's csv as TL4), I can use the expression `$TL4` to reference the field. Additionally, fields can be implicitly combined via element-wise summation via a comma: Referencing `$TL4,TL3` will yield a list that is the sum of all the values of TL4 and TL3, meaning that if TL4 was the list {3, 2} and TL3 was the list {4, 5}, `$TL4,TL3` would yield the list {7, 7}. This notation DOES NOT work inside of a list literal, due to the fact that commas distinguish elements. This limitation can be circomventerd via the use of parentheses (ie. `{($AL4,TL4)}`)

- These references represent the entire column as a list, and operations can be performed on them to derive new fields. In the compute section, for example, you can define new fields to append to your raw data csv. For example, you could add a new field for total cycles with the equation
`$AL1,AL2,AL3,AL4,TL1,TL2,TL3,TL4,ATP,AP,ATB,AB`, which would create a new list that is the sum of all of the constituent fields.
- Because both references and literals are not delimited by spaces, they are space-safe, and cannot be seperated by only a space. `$TN foo bar` will look for the field "TN foo bar" in the data. HOWEVER, fields/literals ARE trimmed, so `$TN        + 1` is the same as `$TN+1` and will NOT look for the field "TN      "  

## Filtration
You can filter lists with conditionals via the `[]` operator. To do this, you can append [<condition>] following a list. For example, to filter the drive rating (DR) list to matches where played defense (D) is true, represented by 1, you can use the expresion `$DR[$D = 1]`, where `=` is tests for equivalence. This works for strings too. For example, the expression `$MN[$TN = 422 & $TC = R1]` represents the list of all match numbers where team 422 is Red 1.
### Conditionals and conditional operators
Conditionals are represented internally by two different types. For list filtration like `$A[$B = x]`, A is indexed with a series of booleans acting as a mask. For normal operations like `3 = 5`, the condition is condensed to a float, with True => 1.0 and False => 0.0. This means that `(2 == 2) + (3 > 2)` is equal to 2, and the above `3 = 5` equals 0. 
The conditional operators avaliable in Beakscript are `=` equals (can also use `==`), `!` not equals (can also use `!=`), `>` greater than, `<` less than, `>=` greater or equal, and `<=` less or equal. Additionally, the `&` and and `|` or operators can be used to compine expressions, and the boolean NOT operator, represented by `!` (not to be confused with the shorthand for !=), can be used to negate boolean expressions (`!(5 > 3)` equals 0.0, which represents False)

## Lists
Apart from using references to make lists (`$A`), you can also manually define lists. This can be useful for packaging operations together. For example, to sum up the total score from algae in a match, you can use the expression `@sum({($ATP,AP), ($ATB,AB)} * {2, 4})`. Because each proceser algae is worth 2 and barge is 4, this expression first adds the total proccessor and barge cycles together with the compound header notation `$A,B`, using parenthesis to avoid instead specifying a list `{$A, B}` where B is a literal because it's a seperate list entry, and then it multiplies the list `{($ATP, AP), ($ATB,AB)}` with the list `{2, 4}`, which turns the list from cycles to score. Finally, the expression uses the unary list operator `@sum` to add together all of the elements in the resultant list, which sums up the score contributions from processor and barge algae.
### List Unary Operators
- `@sum`, adds up all the elements in a list, collapsing it into a single number (`@sum{2, 3}` equals 5)
- `@avg`, returns the average of the list, so `@avg{2, 3}` equals 2.5
- `@min`, returns the minimum value of the list, so `@min{2, 3}` equals 2 
- `@max`, returns the maximum value of the list, so `@max{2, 3}` equals 3
- `@len`, returns the length of the list, so `@len{2, 3}` equals 2

## Operator Precedence
Operator precedence defines the order in which operators are evalutated (order of operations). For example 2 + 2 * 3 is 8, not 12, because the multiplication is carried out before the addition. The order of evaluation of operators is as follows:

- `[]`: this operation is carried out first, but the condition within it is fully evaluated before it is applied
- `- and ! and @sum and @avg and @min and @max and @len`: all unary operators come next, evaluated from left to right 
- `* and / and %`: next is mult/div/mod operators, which are mathematically first by PEMDAS
- `+ and -`: next, add/sub (by PEMDAS)
- `> and < and >= and <=`: gt/lt/ge/le is next
- `=(=) and !(=)`: following the c++ standard, equal and not equal come after >, <, etc.
- `&`: next is and, which is before or by c++ standard
- `|`: finally, or is the last operator evaluated

It is also important to note that parenthesis can be used to short-circut order of operations, even on the `[]` operator (ex. `($TN * $MN)[$TC = R1]`)