package com.liveramp.daemon_lib.executors.processes.execution_conditions.postconfig;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.liveramp.daemon_lib.JobletConfig;

public class ConfigBasedExecutionConditions {

  public static <T extends JobletConfig> ConfigBasedExecutionCondition<T> alwaysExecute() {
    return fromPredicate(Predicates.<T>alwaysTrue());
  }

  public static <T extends JobletConfig> ConfigBasedExecutionCondition<T> fromPredicate(Predicate<T> predicate) {
    return new ConfigBasedExecutionConditionFromPredicate<T>(predicate);
  }

  static class ConfigBasedExecutionConditionFromPredicate<T extends JobletConfig> implements ConfigBasedExecutionCondition<T> {

    private final Predicate<T> predicate;

    ConfigBasedExecutionConditionFromPredicate(Predicate<T> predicate) {
      this.predicate = predicate;
    }

    @Override
    public boolean apply(T input) {
      return predicate.apply(input);
    }
  }
}
