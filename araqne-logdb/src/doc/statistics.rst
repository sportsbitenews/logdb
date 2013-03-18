==================
통계
==================

stats by
==================

count
-----
**usage**

.. parsed-literal::

   **stats count**
   **stats c**

**example**

.. parsed-literal::

  table duration=10m security-events | search port > 1024 | **stats count**

sum
----------

**usage**

.. parsed-literal::

   **stats sum(field)** [as new_field]

**example**

.. parsed-literal::

  table duration=10m security-events | **stats sum(sent)**
  table duration=10m security-events | **stats sum(sent) as sent**



avg
----------

**usage**

.. parsed-literal::

   **stats avg(field)** [as new_field]

**example**

.. parsed-literal::

  table duration=10m security-events | **stats sum(sent)**
  table duration=10m security-events | **stats sum(sent) as sent**

min/max
----------

**usage**

.. parsed-literal::

   **stats min(field)** [as new_field]

   **stats max(field)** [as new_field]

**example**

.. parsed-literal::

  table duration=10m security-events | **stats min(sent)**
  table duration=10m security-events | **stats max(sent) as max**

first/last
----------

**usage**

.. parsed-literal::

   **stats first(field)** [as new_field]
   
   **stats last(field)** [as new_field]

**example**

.. parsed-literal::

  table duration=10m security-events | **stats first(sent)**
  table duration=10m security-events | **stats last(sent) as last**

