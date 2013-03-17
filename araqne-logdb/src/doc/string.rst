===========
문자열 함수
===========

len
==========

**usage:**

.. parsed-literal::

    **len(field_name)**
    
left/right
==========

**usage:**

.. parsed-literal::

    **left(expr, 5)** 
    **right(expr, 5)**
    
**example:**

.. parsed-literal::

    json "{ a: \"helloworld\" }" | eval l = **left(a, 5)** 
        ==> {a=helloworld, l=hello}
    json "{ a: \"helloworld\" }" | eval r = **right("helloworld", 5)** 
        ==> {a=helloworld, r=world}

substr
==========

**usage:**

.. parsed-literal::

    **substr(expr, pos1, pos2)**
    
**example:**

.. parsed-literal::

    json "{ a: \"smiles\" }" | eval l = **substr(a, 1, 5)** 
        ==> {a=smiles, l=mile}

concat
==========

**usage:**

.. parsed-literal::

    **concat(expr, expr)**
    
**example:**

.. parsed-literal::

    json "{ a: \"hello\", b: \"world\" }" | eval result = **concat(a, b)**
        ==> {a=hello, b=world, result=helloworld}
    json "{ a: \"smiles\", b: \"eight\", c: \"away\" }" | eval result = concat(b, " ", right(a, 5), " ", c) 
        ==> {a=smiles, b=eight, c=away, result=eight miles away}

trim
==========

**usage:**

.. parsed-literal::

    **trim(expr)**
    
**example:**

.. parsed-literal::

    json "{ a: \" study hard \" }" | eval result = trim(a)
        ==> {a= study hard , result=study hard}


string
==========

**usage:**

.. parsed-literal::

    **string(expr)** 
    
**example:**

.. parsed-literal::

    query json "{ a: 8, b:\"miles\" }" | eval result = concat(**string(a)**, " ", b)
        ==> {a=8, b=miles, result=8 miles}


match
==========

**usage:**

.. parsed-literal::

    **match(expr, pattern)**
    
**example:**

.. parsed-literal::

    query json "{ a: \"8 miles\" }" | eval result1 = match(a, "\d+ [a-z]+") | eval result2 = match(a, "^[a-z]+$")
        ==> {a=8 miles, result1=true, result2=false}

rex
==========

**usage:**

.. parsed-literal::

    **rex** field=field_name "regular expression"
    
**example:**

.. parsed-literal::
    query json "{ line: \"0;2013-03-01 11:22:33\;f1;f2\" }" | rex field=line "(?<d>\d+-\d+-\d+ \d+:\d+:\d+)" | eval d2 = date(d, "yyyy-MM-dd HH:mm:ss")
        ==> {d=2013-03-01 11:22:33, d2=Fri Mar 01 11:22:33 KST 2013, line=0;2013-03-01 11:22:33;f1;f2}

    query json "{ line: \"2007-10-13 06:20:46 W3SVC1 123.223.21.233 GET /solution/1.982/asp/strawlv01982_msg.asp t=1&m=001921F08323 80 - 125.240.40.73 UtilMind+HTTPGet 404 0 3\" }" | rex field=line "(GET|POST) (?<url>[^ ]*) (?<querystring>[^ ]*) " | fields url, querystring
        ==> {querystring=t=1&m=001921F08323, url=/solution/1.982/asp/strawlv01982_msg.asp}
