==================
통계
==================

stats by
==================

count - 개수 세기
-----------------
**usage**

.. parsed-literal::

   **stats count**
   **stats c**

**example**

.. parsed-literal::

  table duration=10m security-events | search port > 1024 | **stats count**

sum - 필드의 총합 구하기
------------------------

**usage**

.. parsed-literal::

   **stats sum(field)** [as new_field]

**example**

.. parsed-literal::

  table duration=10m security-events | **stats sum(sent)**
  table duration=10m security-events | **stats sum(sent) as sent**



avg - 필드의 평균 구하기
------------------------

**usage**

.. parsed-literal::

   **stats avg(field)** [as new_field]

**example**

.. parsed-literal::

  table duration=10m security-events | **stats sum(sent)**
  table duration=10m security-events | **stats sum(sent) as sent**

min/max - 필드의 최소/최대값 구하기 
-----------------------------------

**usage**

.. parsed-literal::

   **stats min(field)** [as new_field]

   **stats max(field)** [as new_field]

**example**

.. parsed-literal::

  table duration=10m security-events | **stats min(sent)**
  table duration=10m security-events | **stats max(sent) as max**

first/last - 필드의 최초값/최후값 구하기
----------------------------------------

**usage**

.. parsed-literal::

   **stats first(field)** [as new_field]
   
   **stats last(field)** [as new_field]

**example**

.. parsed-literal::

  table duration=10m security-events | **stats first(sent)**
  table duration=10m security-events | **stats last(sent) as last** by s-ip

timechart - 시계열 그래프를 위한 시간대별 통계 값 추출하기
----------------------------------------------------------

**usage**

.. parsed-literal::

   **timechart** span=span_expr aggregation_func [by group_clause]

**example**

.. parsed-literal::

    query textfile offset=5 limit=1000 parser=delimiter delimiter=" " column_headers="date,time,s-sitename,s-ip,cs-method" C:\dev\TEMP\iis.txt | eval _time = date(concat(date, " ", time), "yyyy-MM-dd HH:mm:ss") | **timechart span=1s count** {_time=Sat Oct 13 06:20:46 KST 2007, count=21}
            {_time=Sat Oct 13 06:20:47 KST 2007, count=22}
            {_time=Sat Oct 13 06:20:48 KST 2007, count=17}
            {_time=Sat Oct 13 06:20:49 KST 2007, count=24}
            {_time=Sat Oct 13 06:20:50 KST 2007, count=18}
            {_time=Sat Oct 13 06:20:51 KST 2007, count=18}

     query textfile offset=5 limit=10000 parser=delimiter delimiter=" " column_headers="date,time,s-sitename,s-ip,cs-method,u,q,p,un,c-ip" C:\dev\TEMP\iis.txt | eval _time = date(concat(date, " ", time), "yyyy-MM-dd HH:mm:ss") | **timechart span=1s count by u**
            {str82_msg.asp=21, _time=Sat Oct 13 06:20:46 KST 2007}
            {str82_msg.asp=22, _time=Sat Oct 13 06:20:47 KST 2007}
            {str82_msg.asp=17, _time=Sat Oct 13 06:20:48 KST 2007}
            {str82_msg.asp=24, _time=Sat Oct 13 06:20:49 KST 2007}
            {str82_msg.asp=18, _time=Sat Oct 13 06:20:50 KST 2007}
            {str82_msg.asp=18, _time=Sat Oct 13 06:20:51 KST 2007}
            {str82_msg.asp=20, _time=Sat Oct 13 06:20:52 KST 2007}
            {str82_msg.asp=16, _time=Sat Oct 13 06:20:53 KST 2007}
            {str82_log.asp=3, str82_msg.asp=28, _time=Sat Oct 13 06:20:54 KST 2007}
            {str82_msg.asp=18, _time=Sat Oct 13 06:20:55 KST 2007}
            {str82_log.asp=1, str82_msg.asp=8, _time=Sat Oct 13 06:20:56 KST 2007}
            {str82_msg.asp=28, _time=Sat Oct 13 06:20:57 KST 2007}
            {str82_msg.asp=25, _time=Sat Oct 13 06:20:59 KST 2007}
            {str82_msg.asp=14, _time=Sat Oct 13 06:21:00 KST 2007}
            {content.asp=1, str82_log.asp=3, str82_msg.asp=17, _time=Sat Oct 13 06:21:01 KST 2007}
            {str82_log.asp=2, str82_msg.asp=18, _time=Sat Oct 13 06:21:02 KST 2007}
            {str82_msg.asp=13, _time=Sat Oct 13 06:21:03 KST 2007}
            {str82_log.asp=1, str82_msg.asp=13, _time=Sat Oct 13 06:21:04 KST 2007}
            {str82_msg.asp=20, _time=Sat Oct 13 06:21:05 KST 2007}
            {str82_msg.asp=20, _time=Sat Oct 13 06:21:06 KST 2007}
            {str82_msg.asp=19, _time=Sat Oct 13 06:21:07 KST 2007}
            {str82_log.asp=2, str82_msg.asp=18, _time=Sat Oct 13 06:21:08 KST 2007}
            {str82_msg.asp=9, _time=Sat Oct 13 06:21:09 KST 2007}
            {ad.ini=1, str82_msg.asp=22, channel/channel.ini=23, notice.ini=24, _time=Sat Oct 13 06:21:10 KST 2007}
            {str82_msg.asp=17, _time=Sat Oct 13 06:21:11 KST 2007}
            {str82_msg.asp=7, _time=Sat Oct 13 06:21:12 KST 2007}
            {str82_msg.asp=14, channel/channel.ini=15, _time=Sat Oct 13 06:21:13 KST 2007}
            {ad.ini=1, str82_msg.asp=20, notice.ini=21, _time=Sat Oct 13 06:21:14 KST 2007}
            {str82_msg.asp=21, _time=Sat Oct 13 06:21:15 KST 2007}
            {str82_msg.asp=20, _time=Sat Oct 13 06:21:16 KST 2007}
            {str82_msg.asp=20, _time=Sat Oct 13 06:21:17 KST 2007}
            {ad.ini=1, str82_msg.asp=18, channel/channel.ini=19, notice.ini=20, _time=Sat Oct 13 06:21:19 KST 2007}
            {str82_msg.asp=23, _time=Sat Oct 13 06:21:20 KST 2007}
            {str82_log.asp=1, str82_msg.asp=20, _time=Sat Oct 13 06:21:21 KST 2007}
            {str82_msg.asp=11, _time=Sat Oct 13 06:21:22 KST 2007}
            {str82_log.asp=2, str82_msg.asp=17, _time=Sat Oct 13 06:21:23 KST 2007}
            {str82_log.asp=1, str82_msg.asp=19, _time=Sat Oct 13 06:21:24 KST 2007}
            {str82_msg.asp=11, _time=Sat Oct 13 06:21:25 KST 2007}
            {str82_msg.asp=27, _time=Sat Oct 13 06:21:26 KST 2007}
            {str82_msg.asp=15, _time=Sat Oct 13 06:21:27 KST 2007}

