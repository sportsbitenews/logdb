==================
통계
==================

stats by
==================

갯수 - count
-----
**usage**

.. parsed-literal::

   **stats count**
   **stats c**

**example**

.. parsed-literal::

  table duration=10m security-events | search port > 1024 | **stats count**

합계 - sum
----------

**usage**

.. parsed-literal::

   **stats sum(field)** as new_field

**example**

.. parsed-literal::

  table duration=10m security-events | **stats sum(sent)**
  table duration=10m security-events | **stats sum(sent) as sent**



평균값 - avg
----------

**usage**

.. parsed-literal::

   **stats avg(field)** as new_field

**example**

.. parsed-literal::

  table duration=10m security-events | **stats sum(sent)**
  table duration=10m security-events | **stats sum(sent) as sent**

최대값 - max
----------

**usage**

.. parsed-literal::

   **stats max(field)**
   **stats max(field)** as new_field

**example**

.. parsed-literal::

  table duration=10m security-events | **stats max(sent)**
  table duration=10m security-events | **stats max(sent) as max**

최소값 - min
----------

**usage**

.. parsed-literal::

   **stats min(field)**
   **stats min(field)** as new_field

**example**

.. parsed-literal::

  table duration=10m security-events | **stats min(sent)**
  table duration=10m security-events | **stats min(sent) as min**

첫번째 값 - first
----------

**usage**

.. parsed-literal::

   **stats first(field)**
   **stats first(field)** as new_field

**example**

.. parsed-literal::

  table duration=10m security-events | **stats first(sent)**
  table duration=10m security-events | **stats first(sent) as fisrt**

마지막 값 - last
----------

최대값과 최소값의 차이값 - range
----------


per_seond
----------

per_minute
----------

per_hour
----------

per_day
----------


