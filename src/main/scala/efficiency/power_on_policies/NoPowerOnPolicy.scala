package efficiency.power_on_policies

import efficiency.power_on_policies.action.{DefaultPowerOnAction, PowerOnAction}
import efficiency.power_on_policies.decision.{NoPowerOnDecision, PowerOnDecision}

/**
 * Created by dfernandez on 13/1/16.
 */
object NoPowerOnPolicy extends PowerOnPolicy{

  override var powerOnAction: PowerOnAction = DefaultPowerOnAction
  override var powerOnDecisionPolicy: PowerOnDecision = NoPowerOnDecision

  override def getName(): String = {
    "no-power-on-policy"
  }
}
