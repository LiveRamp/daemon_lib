package com.liveramp.daemon_lib.executors.processes.execution_conditions.preconfig;

public class ExecutionConditions {
  public static ExecutionCondition and(ExecutionCondition condition1, ExecutionCondition condition2) {
    return new AndCompositeExecutionCondition(condition1, condition2);

  }

  public static ExecutionCondition or(ExecutionCondition condition1, ExecutionCondition condition2) {
    return new OrCompositeExecutionCondition(condition1, condition2);

  }

  public static ExecutionCondition alwaysExecute() {
    return new AlwaysExecuteCondition();
  }

  static class AlwaysExecuteCondition implements ExecutionCondition {

    @Override
    public boolean canExecute() {
      return true;
    }
  }

  static class AndCompositeExecutionCondition implements ExecutionCondition {
    private final ExecutionCondition condition1;
    private final ExecutionCondition condition2;

    AndCompositeExecutionCondition(ExecutionCondition condition1, ExecutionCondition condition2) {
      this.condition1 = condition1;
      this.condition2 = condition2;
    }

    @Override
    public boolean canExecute() {
      return condition1.canExecute() && condition2.canExecute();
    }
  }

  static class OrCompositeExecutionCondition implements ExecutionCondition {
    private final ExecutionCondition condition1;
    private final ExecutionCondition condition2;

    OrCompositeExecutionCondition(ExecutionCondition condition1, ExecutionCondition condition2) {
      this.condition1 = condition1;
      this.condition2 = condition2;
    }

    @Override
    public boolean canExecute() {
      return condition1.canExecute() || condition2.canExecute();
    }
  }
}
