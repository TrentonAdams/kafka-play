package com.example.project


import spock.lang.Specification

class CalculatorTests extends Specification
{
    def "add two numbers"()
    {
        given:
        Calculator calculator = new Calculator();

        expect:
        calculator.add(first, second) == sum

        where:
        first | second | sum
        0     | 1      | 1
        1     | 2      | 3
        49    | 51     | 100
        1     | 100    | 101
    }
}
