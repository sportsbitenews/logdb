=====================
필드 조작 명령어
=====================

eval
================

**usage:**

.. parsed-literal::

    **eval** new_field = expression

**example:**

.. parsed-literal::

    eval _time = string(_time, "yyyy-MM-dd HH:mm")
    eval total = sent + rcvd
    eval note = if (len(note)>10, concat(left(note, 10), "..."), note)

fields
================

**usage:**

.. parsed-literal::

    **fields** field_name[, field_name]...
    
**example:**

.. parsed-literal::

    json "{ a: 1, b: 2, c: 3 }" | fields a, c
        ==> {a=1, c=3}

rename
================

**usage:**

.. parsed-literal::

    **rename** old_field_name new_field_name
    
**example:**

.. parsed-literal::
 
    json "[ { a: 42 }, { a: 52 } ]" | rename a as c
        ==> {c=42}
            {c=52}


lookup
================

**usage:**

.. parsed-literal::

    **lookup** lookup_table field_lookup_in **output** field_name[, field_name]...
    
**example:**

.. parsed-literal::



=====================
쿼리 결과 변환 명령어 
=====================

outputcsv
===============

**usage:**

.. parsed-literal::

    **outputcsv** field_name[, field_name]...
    
**example:**

.. parsed-literal::

    json "[ { a: 42, b: 12 }, { a: 52, b: 22 } ]" | outputcsv C:\\temp\\a.csv a b

then it creates ``C:\TEMP\a.csv`` with column a, b like following.

a.csv::

    a,b
    42,12
    52,22


