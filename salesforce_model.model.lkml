connection: "bq_gelato_data_warehouse"

include: "*.view.lkml"         # include all views in this project
include: "*.dashboard.lookml"  # include all dashboards in this project
# preliminaries #


# views to explore——i.e., "base views" #

explore: historical_snapshot    {
  label: "Historical Opportunity Snapshot"

  join: opportunity{
    view_label: "Current Opportunity State"
    sql_on: ${historical_snapshot.opportunityid} = ${opportunity.id};;
    type: inner
    relationship: many_to_one
  }
  join: sf_account{
    sql_on: ${opportunity.account_id} = ${sf_account.id};;
    relationship: many_to_one
    type: inner
  }
  join: opportunity_facts{
    view_label: "Account"
    sql_on: ${opportunity.account_id} = ${opportunity_facts.account_id};;
    relationship: many_to_one
    type: inner
  }
  join: account_owner{
    from: sf_user
    sql_on: ${sf_account.ownerid} = ${account_owner.id};;
    relationship: many_to_one
    type: inner
  }
}
