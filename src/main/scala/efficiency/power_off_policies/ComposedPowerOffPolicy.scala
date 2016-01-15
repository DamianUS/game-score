package efficiency.power_off_policies

import efficiency.power_off_policies.action.PowerOffAction
import efficiency.power_off_policies.decision.PowerOffDecision

/**
 * Created by dfernandez on 13/1/16.
 */
class ComposedPowerOffPolicy(action : PowerOffAction, decision : PowerOffDecision) extends PowerOffPolicy{

  override var powerOffAction: PowerOffAction = action
  override var powerOffDecisionPolicy: PowerOffDecision = decision

  override val name: String = ("Composed Power Off Policy with decision: %s and action: %s").format(powerOffDecisionPolicy.name, powerOffAction.name)

}
