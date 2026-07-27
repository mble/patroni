Feature: postgres exec prefix
  We should check that PostgreSQL started through postgresql.postgres_exec_prefix behaves exactly like a normal one.

  Scenario: check bootstrap and replication through the exec prefix
    Given I configure and start postgres-0 with a postgres exec prefix
    Then postgres-0 is a leader after 10 seconds
    And there is a non empty initialize key in DCS after 15 seconds
    When I configure and start postgres-1 with a postgres exec prefix
    And I add the table foo to postgres-0
    Then table foo is present on postgres-1 after 20 seconds
    And the exec prefix of postgres-0 recorded 1 or more postgres executions

  Scenario: check restart through the exec prefix
    Given I run patronictl.py restart batman postgres-1 --force
    Then postgres-1 role is the replica after 10 seconds
    And replication works from postgres-0 to postgres-1 after 15 seconds
    And the exec prefix of postgres-1 recorded 2 or more postgres executions

  Scenario: check crash detection and recovery through the exec prefix
    Given I kill postmaster on postgres-0
    Then postgres-0 role is the primary after 10 seconds
    And "members/postgres-0" key in DCS has state=running after 12 seconds
    And replication works from postgres-0 to postgres-1 after 15 seconds
    And the exec prefix of postgres-0 recorded 2 or more postgres executions

  Scenario: check that Patroni reattaches to a postmaster started through the exec prefix
    Given I kill postgres-1
    When I start postgres-1
    Then "members/postgres-1" key in DCS has state=running after 20 seconds
    And replication works from postgres-0 to postgres-1 after 15 seconds

  Scenario: check clean shutdown and start of a postmaster started through the exec prefix
    Given I shut down postgres-1
    When I start postgres-1
    Then "members/postgres-1" key in DCS has state=running after 20 seconds
    And replication works from postgres-0 to postgres-1 after 15 seconds
    And the exec prefix of postgres-1 recorded 3 or more postgres executions
