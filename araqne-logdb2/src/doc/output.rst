=====================
필드 조작 명령어
=====================

eval - 필드에 계산식의 결과값을 대입
====================================

**usage:**

.. parsed-literal::

    **eval** new_field = expression

**example:**

.. parsed-literal::

    eval _time = string(_time, "yyyy-MM-dd HH:mm")
    eval total = sent + rcvd
    eval note = if (len(note)>10, concat(left(note, 10), "..."), note)

fields - 필요한 필드만 추려냄 
=============================

**usage:**

.. parsed-literal::

    **fields** field_name[, field_name]...
    
**example:**

.. parsed-literal::

    json "{ a: 1, b: 2, c: 3 }" | fields a, c
        ==> {a=1, c=3}

rename - 필드 이름을 바꿔 사용함 
================================

**usage:**

.. parsed-literal::

    **rename** old_field_name new_field_name
    
**example:**

.. parsed-literal::
 
    json "[ { a: 42 }, { a: 52 } ]" | rename a as c
        ==> {c=42}
            {c=52}


lookup - 별도의 매핑 테이블에서 값을 조회하여 필드를 추가함
===========================================================

**usage:**

.. parsed-literal::

    **lookup** lookup_table field_lookup_in **output** field_name[, field_name]...

``lookup_table`` 로드는 현재 Araqne 콘솔에서 ``logdb.loadCsvLookup`` 명령으로만 가능합니다. 
    
**example:**

.. parsed-literal::

    ::hosts.csv::
    ip,hostname
    127.0.0.1,localhost
    1.2.3.4,host1
    2.3.4.5,host2

    **json** "[ { ip: \\"1.2.3.4\\" }, { ip: \\"2.3.4.5\\" } ]" | lookup csv$hosts.csv ip output hostname
        ==> {hostname=host1, ip=1.2.3.4}
            {hostname=host2, ip=2.3.4.5}



=====================
쿼리 결과 변환 명령어 
=====================

outputcsv - csv 파일로 결과를 저장함
====================================

**usage:**

.. parsed-literal::

    **outputcsv** field_name[, field_name]...
    
**example:**

.. parsed-literal::

    json "[ { a: 42, b: 12 }, { a: 52, b: 22 } ]" | outputcsv C:\\temp\\a.csv a b

위 명령을 이용하면 다음과 같이 a, b 필드의 내용을 담은 ``C:\TEMP\a.csv`` 파일이 생성됩니다. 

a.csv::

    a,b
    42,12
    52,22


