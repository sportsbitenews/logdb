==============
로그 쿼리 기본
==============

::

    데이터 소스 커맨드 -> 필터링/통계/가공 커맨드 [-> 결과집합 커맨드]


table - 테이블 쿼리 
===================

**usage:**

.. parsed-literal::

    **table** [option=value] table_name
    
**options:**

- offset
- limit
- duration: 1s, 1m, 1h, 1d, 1mon
- from: yyyyMMddHHmmss
- to: yyyyMMddHHmmss

**example:**

.. parsed-literal::

    table limit=100000 security-events
    table duration=1h security-events
    table from=20121201 to=20121203 security-events



search - 필터링 
===============

**usage:**

.. parsed-literal::

    **search** expression

**example:**

.. parsed-literal::

    table duration=10m security-events | **search ip == "192.168.*"**
    table duration=10m security-events | **search port > 1024**
    table duration=10m security-events | **search isnotnull(nat_sip)**
    table duration=10m security-events | **search (rcvd + sent) > 10000000**


eval - 필드 할당
================

**usage:**

.. parsed-literal::

    **eval** new_field = expression

**example:**

.. parsed-literal::

    eval _time = string(_time, "yyyy-MM-dd HH:mm")
    eval total = sent + rcvd
    eval note = if (len(note)>10, concat(left(note, 10), "..."), note)


