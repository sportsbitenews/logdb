===============
입력 명령어
===============

table - logstorage의 테이블로부터 데이터 읽기
---------------------------------------------

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

    **table** limit=100000 security-events
    **table** duration=1h security-events
    **table** from=20121201 to=20121203 security-events


textfile - 텍스트 파일로부터 데이터 읽기
----------------------------------------

**usage:**

.. parsed-literal::

    **textfile** [option=value] file_path
    
**options:**

- offset
- limit
- parser: parser factory name

지정된 파서에 필요한 옵션도 추가해줘야 합니다.

**example:**

.. parsed-literal::

    **textfile** limit=100000 C:\\TEMP\\iis.txt
    **textfile** parser=delimiter delimiter="; " column_headers="a,b,c,d,e" C:\\TEMP\\iis.txt
    **textfile** offset=5 limit=10 parser=delimiter delimiter=" " column_headers="date,time,s-sitename,s-ip,cs-method" C:\\TEMP\\iis.txt | fields date, time, s-sitename,s-ip, cs-method
         ==> {cs-method=GET, date=2007-10-13, s-ip=1.2.3.4, s-sitename=W3SVC1, time=06:20:46}
             {cs-method=GET, date=2007-10-13, s-ip=1.2.3.4, s-sitename=W3SVC1, time=06:20:46}
             {cs-method=GET, date=2007-10-13, s-ip=1.2.3.4, s-sitename=W3SVC1, time=06:20:46}
             {cs-method=GET, date=2007-10-13, s-ip=1.2.3.4, s-sitename=W3SVC1, time=06:20:46}
             {cs-method=GET, date=2007-10-13, s-ip=1.2.3.4, s-sitename=W3SVC1, time=06:20:46}
             {cs-method=GET, date=2007-10-13, s-ip=1.2.3.4, s-sitename=W3SVC1, time=06:20:46}
             {cs-method=GET, date=2007-10-13, s-ip=1.2.3.4, s-sitename=W3SVC1, time=06:20:46}
             {cs-method=GET, date=2007-10-13, s-ip=1.2.3.4, s-sitename=W3SVC1, time=06:20:46}
             {cs-method=GET, date=2007-10-13, s-ip=1.2.3.4, s-sitename=W3SVC1, time=06:20:46}
             {cs-method=GET, date=2007-10-13, s-ip=1.2.3.4, s-sitename=W3SVC1, time=06:20:46}




zipfile - zip 형식으로 압축된 파일로부터 데이터 읽기
----------------------------------------------------

**usage:**

.. parsed-literal::

    **textfile** [option=value] zip_file_path textfile_in_zip
    
**options:**

``textfile`` 명령과 기본적인 틀은 같으나 ``parser`` 관련 명령을 지정할 수 없습니다.

- offset
- limit

**example:**

.. parsed-literal::

    **zipfile** limit=100000 c:\\TEMP\\iis.zip iis.txt

    **zipfile** offset=5 limit=10 C:\\TEMP\\iis.zip iis.txt
        ==> {line=2007-10-13 06:20:46 W3SVC1 1.2.3.4 GET /solution/str82_msg.asp t=1&m=65 80 - 4.5.6.7 UtilMind+HTTPGet 404 0 3}
            {line=2007-10-13 06:20:46 W3SVC1 1.2.3.4 GET /solution/str82_msg.asp t=1&m=68 80 - 4.5.6.10 UtilMind+HTTPGet 404 0 3}
            {line=2007-10-13 06:20:46 W3SVC1 1.2.3.4 GET /solution/str82_msg.asp t=1&m=AD 80 - 4.6.7.7 UtilMind+HTTPGet 404 0 3}
            {line=2007-10-13 06:20:46 W3SVC1 1.2.3.4 GET /solution/str82_msg.asp t=1&m=01 80 - 4.5.6.8 UtilMind+HTTPGet 404 0 3}
            {line=2007-10-13 06:20:46 W3SVC1 1.2.3.4 GET /solution/str82_msg.asp t=1&m=04 80 - 4.5.7.8 UtilMind+HTTPGet 404 0 3}
            {line=2007-10-13 06:20:46 W3SVC1 1.2.3.4 GET /solution/str82_msg.asp t=1&m=1D 80 - 4.5.6.116 UtilMind+HTTPGet 404 0 3}
            {line=2007-10-13 06:20:46 W3SVC1 1.2.3.4 GET /solution/str82_msg.asp t=1&m=70 80 - 4.6.7.8 UtilMind+HTTPGet 404 0 3}
            {line=2007-10-13 06:20:46 W3SVC1 1.2.3.4 GET /solution/str82_msg.asp t=1&m=02 80 - 4.5.6.13 UtilMind+HTTPGet 404 0 3}
            {line=2007-10-13 06:20:46 W3SVC1 1.2.3.4 GET /solution/str82_msg.asp t=1&m=72 80 - 4.5.6.30 UtilMind+HTTPGet 404 0 3}
            {line=2007-10-13 06:20:46 W3SVC1 1.2.3.4 GET /solution/str82_msg.asp t=1&m=3B 80 - 4.5.6.47 UtilMind+HTTPGet 404 0 3}


json - json 문자열로부터 데이터 생성하기
----------------------------------------

**usage:**

.. parsed-literal::

    **json** "valid_json_object"
    **json** "valid_json_array"
    
**example:**

.. parsed-literal::

    **json "{ a: 8, b:\"miles\" }"** | eval result = concat(string(a), " ", b)
        ==> {a=8, b=miles, result=8 miles}

    **json "[ { a: 84 }, { a: 72 }, { a: 42 } ]"** | eval result = case(a > 90, "A", a > 80, "B", a > 70, "C", "F")
        ==> {a=84, result=B}
            {a=72, result=C}
            {a=42, result=F}


