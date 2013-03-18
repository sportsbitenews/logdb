===================
조건 처리문
===================

if - 조건에 따른 대입 
============================

**usage:**

.. parsed-literal::

    **if(boolean_expr, expr_if_true, expr_if_false)** 
    
**example:**

.. parsed-literal::

    json "[ { a: 42 }, { a: 52 } ]" | eval result = **if(a < 50, "true", "false")**
        ==> {a=42, result=true}
        ==> {a=52, result=false}

case - 여러 조건을 한번에 처리하기
==================================

**usage:**

.. parsed-literal::

    **case(boolean_expr, expr[, boolean_expr, expr]..., default_expr)** 
    
**example:**

.. parsed-literal::
    json "[ { a: 84 }, { a: 72 }, { a: 42 } ]" | eval result = case(a > 90, "A", a > 80, "B", a > 70, "C", "F")
        ==> {a=84, result=B}
            {a=72, result=C}
            {a=42, result=F}



