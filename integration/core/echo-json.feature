Feature: Cmd Line Echo Task
  Test Echo task using command line runner and JSON serde

  Scenario: JSON serde
    Given the task config file
    """
    config/echo-json.cmdline.properties
    """
      When stdin is
      """
      {"mumbo": "jumbo1"}
      {"mumbo": "jumbo2"}
      """
      Then stdout should be
      """
      {"mumbo": "jumbo1"}
      {"mumbo": "jumbo2"}
      """

  Scenario: JSON serde with bad input
    Given the task config file
    """
    config/echo-json.cmdline.properties
    """
      When stdin is
      """
      {"mumbo": "jumbo1"
      """
      Then task should fail

  Scenario: Avro serde
    Given the task config file
    """
    config/echo-avro.cmdline.properties
    """
      When stdin is
      """
      {"mumbo": "jumbo1"}
      {"mumbo": "jumbo2"}
      """
      Then stdout should be
      """
      {"mumbo": "jumbo1"}
      {"mumbo": "jumbo2"}
      """

  Scenario: Avro serde with bad input
    Given the task config file
    """
    config/echo-avro.cmdline.properties
    """
      When stdin is
      """
      {"mumbo2": "jumbo1"}
      """
      Then task should fail
