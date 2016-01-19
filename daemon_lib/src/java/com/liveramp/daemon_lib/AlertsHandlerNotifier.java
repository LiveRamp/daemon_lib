package com.liveramp.daemon_lib;

import com.google.common.base.Optional;

import com.liveramp.java_support.alerts_handler.AlertsHandler;
import com.liveramp.java_support.alerts_handler.recipients.AlertRecipient;
import com.liveramp.java_support.alerts_handler.recipients.AlertRecipients;
import com.liveramp.java_support.alerts_handler.recipients.AlertSeverity;

public class AlertsHandlerNotifier implements DaemonNotifier {
  private static final AlertSeverity DEFAULT_SEVERITY = AlertSeverity.ERROR;
  private static final AlertRecipient DEFAULT_RECEPIENT = AlertRecipients.engineering(DEFAULT_SEVERITY);

  private final AlertsHandler alertsHandler;
  private static AlertRecipient alertRecipient;

  public AlertsHandlerNotifier(AlertsHandler alertsHandler) {
    this(alertsHandler, DEFAULT_RECEPIENT);
  }

  public AlertsHandlerNotifier(AlertsHandler alertsHandler, AlertRecipient alertRecipient) {
    this.alertsHandler = alertsHandler;
    this.alertRecipient = alertRecipient;
  }

  public void notify(String subject, Optional<String> body, Optional<? extends Throwable> t) {
    if (body.isPresent()) {
      if (t.isPresent()) {
        alertsHandler.sendAlert(subject, body.get(), t.get(), alertRecipient);
      }
      else {
        alertsHandler.sendAlert(subject, body.get(), alertRecipient);
      }
    }
    else {
      if (t.isPresent()) {
        alertsHandler.sendAlert(subject, t.get(), alertRecipient);
      }
    }
  }
}
