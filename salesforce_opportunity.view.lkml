view: historical_snapshot {
  derived_table:{
    sql:
      WITH snapshot_window as ( SELECT opportunity_history.*
                                  , coalesce(lead(createddate,1) over(partition by opportunityid ORDER BY createddate), current_timestamp) as stage_end
                                FROM salesforce.sf_OpportunityHistory AS opportunity_history
                              )
      SELECT CAST(dates.date as timestamp) as observation_date
        , snapshot_window.*
      FROM snapshot_window
      LEFT JOIN ${dates.SQL_TABLE_NAME} as dates
      ON dates.date = CAST(snapshot_window.createddate as date)
      AND dates.date = CAST(snapshot_window.stage_end as date)
      WHERE dates.date = CAST(current_date as date)
      ;;

    # persist_for: "24 hours"
    }
    dimension: id{
      type: string
      primary_key: yes
      sql: ${TABLE}.id;;
    }
    dimension: opportunityid{
      type: string
      hidden: yes
      sql: ${TABLE}.opportunityid;;
    }
    dimension_group: snapshot{
      type: time
      description: "What snapshot date are you interetsed in?"
      timeframes: [time, date, week, month]
      sql: ${TABLE}.observation_date;;
    }

    dimension_group: created{
      type: time
      hidden: yes
      timeframes: [time, date, week, month]
      sql: ${TABLE}.created_at;;
    }

    dimension_group: close{
      type: time
      description: "At the time of snapshot, what was the projected close date?"
      timeframes: [date, week, month]
      sql: ${TABLE}.close_date;;
    }

    dimension_group: stage_end{
      type: time
      hidden: yes
      timeframes: [time, date, week, month]
      sql: ${TABLE}.stage_end;;
    }

    dimension: expected_revenue{
      type: number
      sql: ${TABLE}.expected_revenue;;
    }

    dimension: amount{
      type: number
      sql: ${TABLE}.amount;;
    }

    dimension: stage_name{
      type: string
      hidden: yes
      sql: ${TABLE}.stagename;;
    }

    dimension: stage_name_funnel{
      description: "At the time of snapshot, what funnel stage was the prospect in?"
#     sql_case:
#       Lead: ${stage_name} = 'Active Lead'
#       Prospect: ${stage_name} ilike '%Prospect%'
#       Trial: ${stage_name} ilike '%Trial%'
#       Winning:  ${stage_name} IN ('Proposal','Commit- Not Won','Negotiation')
#       Won:  ${stage_name} = 'Closed Won'
#       Lost: ${stage_name} ilike '%Closed%'
#       Unknown: yes

      case: {
        when: {
          label: "Appointment"
          sql: ${stage_name} = "Appointment Set" ;;
        }

        when: {
          label: "Qualification"
          sql: ${stage_name} = "Appointment Set" ;;
        }
        when: {
          label: "Proposal"
          sql: ${stage_name} = "Appointment Set" ;;
        }
        when: {
          label: "Negotiation"
          sql: ${stage_name} = "Appointment Set" ;;
        }
        when: {
          label: "PO Raised"
          sql: ${stage_name} = "Appointment Set" ;;
        }
        when: {
          label: "Closed Won"
          sql: ${stage_name} = "Appointment Set" ;;
        }
        else: "Closed Lost"
      }
    }
    dimension: probability{
      type: number
      sql: ${TABLE}.probability;;
    }

    dimension: probability_tier{
      sql:
      CASE WHEN ${probability} = 100 THEN "WON"
      WHEN ${probability} >= 80 THEN "80 - 99%"
      WHEN ${probability} >= 60 THEN "60 - 79%"

      WHEN ${probability} >= 40 THEN "40 - 59%"
      WHEN ${probability} >= 20 THEN "20 - 39%"
       WHEN ${probability} > 0 THEN "1 - 19%"
      ELSE "LOST" END;;
    }

# measures #

    measure: total_amount{
      type: sum
      description: "At the time of snapshot, what was the total projected ACV?"
      sql: ${amount};;
      value_format: "$#,##0"
      drill_fields: [account.name, snapshot_date, close_date, amount, probability, stage_name_funnel]
    }
    measure: count_opportunities{
      type: count_distinct
      sql: ${opportunityid};;
    }

# sets #

    set: detail {
      fields: [snapshot_date,
        id,
        opportunityid,
        expected_revenue,
        amount,
        stage_name,
        close_date,
        stage_end_date]
    }
  }
# Derived table of numbers and dates (Redshift Implementation)
  view: two_numbers{
    derived_table:{
      sql: SELECT 1 as num UNION ALL SELECT 2 as num;;
      # persist_for: "500 hours"
    }
  }
  view: numbers {
    derived_table: {
      # persist_for: "500 hours"
      sql:
        SELECT
          ROW_NUMBER() OVER (ORDER BY a2.num ) as number
        FROM ${two_numbers.SQL_TABLE_NAME} as a2,
            ${two_numbers.SQL_TABLE_NAME} as a4,
             ${two_numbers.SQL_TABLE_NAME} as a8,
             ${two_numbers.SQL_TABLE_NAME} as a16,
             ${two_numbers.SQL_TABLE_NAME} as a32,
             ${two_numbers.SQL_TABLE_NAME} as a64,
             ${two_numbers.SQL_TABLE_NAME} as a128,
             ${two_numbers.SQL_TABLE_NAME} as a256,
             ${two_numbers.SQL_TABLE_NAME} as a512,
             ${two_numbers.SQL_TABLE_NAME} as a1024,
             ${two_numbers.SQL_TABLE_NAME} as a2048,
             ${two_numbers.SQL_TABLE_NAME} as a4096,
             ${two_numbers.SQL_TABLE_NAME} as a8192,
             ${two_numbers.SQL_TABLE_NAME} as a16384,
             ${two_numbers.SQL_TABLE_NAME} as a32768,
             ${two_numbers.SQL_TABLE_NAME} as a65535,
             ${two_numbers.SQL_TABLE_NAME} as a131072,
             ${two_numbers.SQL_TABLE_NAME} as a262144
            ;;
    }
    dimension: number{
      primary_key: yes
      type: number
    }
  }
  view: dates {
    derived_table:{
      # persist_for: "500 hours"
      sql:
      SELECT DATE_FROM_UNIX_DATE(number) as date FROM ${numbers.SQL_TABLE_NAME} as numbers;;
    }

    dimension_group: event {
      type: time
      timeframes: [date, week, month, year]
      convert_tz: no
      sql: ${TABLE}.date
        ;;
    }
  }
  view: _opportunity{
    sql_table_name: salesforce.sf_Opportunity ;;

# Dimensions

    dimension: id {
      primary_key: yes
      type: string
      sql: ${TABLE}.id;;
    }
    dimension: account_id {
      type: string
      hidden: yes
      sql: ${TABLE}.account_id;;
    }
    dimension: amount {
      type: number
      sql: ${TABLE}.amount;;
    }
    dimension: campaign_id {
      type: string
      hidden: yes
      sql: ${TABLE}.campaign_id;;
    }
    dimension_group: close {
      type: time
      timeframes: [date, week, month]
      convert_tz: no
      sql: ${TABLE}.close_date;;
    }
    dimension: created_by_id {
      type: string
      hidden: yes
      sql: ${TABLE}.created_by_id;;
    }
    dimension_group: created {
      type: time
      timeframes: [date, week, month]
      sql: ${TABLE}.createddate;;
    }
    dimension: description {
      type: string
      sql: ${TABLE}.description;;
    }
    dimension: fiscal {
      type: string
      sql: ${TABLE}.fiscal;;
    }
    dimension: fiscal_quarter {
      type: number
      sql: ${TABLE}.fiscal_quarter;;
    }
    dimension: fiscal_year {
      type: number
      sql: ${TABLE}.fiscal_year;;
    }
    dimension: forecast_category {
      type: string
      sql: ${TABLE}.forecast_category;;
    }
    dimension: forecast_category_name {
      type: string
      sql: ${TABLE}.forecast_category_name;;
    }
    dimension: has_opportunity_line_item {
      type: yesno
      sql: ${TABLE}.has_opportunity_line_item;;
    }
    dimension: is_closed {
      type: yesno
      sql: ${TABLE}.is_closed;;
    }
    dimension: is_deleted {
      type: yesno
      sql: ${TABLE}.is_deleted;;
    }
    dimension: is_won {
      type: yesno
      sql: ${TABLE}.is_won;;
    }
    dimension_group: last_activity {
      type: time
      timeframes: [date, week, month]
      convert_tz: no
      sql: ${TABLE}.last_activity_date;;
    }
    dimension: last_modified_by_id {
      type: string
      hidden: yes
      sql: ${TABLE}.last_modified_by_id;;
    }
    dimension_group: last_modified {
      type: time
      timeframes: [date, week, month]
      sql: ${TABLE}.last_modified_date;;
    }
    dimension_group: last_referenced {
      type: time
      timeframes: [date, week, month]
      sql: ${TABLE}.last_referenced_date;;
    }
    dimension_group: last_viewed {
      type: time
      timeframes: [date, week, month]
      sql: ${TABLE}.last_viewed_date;;
    }
    dimension: lead_source {
      type: string
      sql: ${TABLE}.lead_source;;
    }
    dimension: name {
      type: string
      sql: ${TABLE}.name;;
    }
    dimension: owner_id {
      type: string
      hidden: yes
      sql: ${TABLE}.owner_id;;
    }
    dimension: pricebook_2_id {
      type: string
      hidden: yes
      sql: ${TABLE}.pricebook_2_id;;
    }
    dimension: probability {
      type: number
      sql: ${TABLE}.probability;;
    }
    dimension: stage_name {
      type: string
      sql: ${TABLE}.stage_name;;
    }
    dimension: synced_quote_id {
      type: string
      hidden: yes
      sql: ${TABLE}.synced_quote_id;;
    }
    dimension_group: system_modstamp {
      type: time
      timeframes: [date, week, month]
      sql: ${TABLE}.system_modstamp;;
    }
    dimension: type {
      type: string
      sql: ${TABLE}.type;;
    }
# measures #

    measure: count{
      type: count
      drill_fields: [id, name, stage_name, forecast_category_name]

    }}
  view: _opportunity_history{
    sql_table_name: salesforce.sf_OpportunityHistory;;

# dimensions #

    dimension: id{
      primary_key: yes
      sql: ${TABLE}.id;;
    }
    dimension: amount{
      type: number
      sql: ${TABLE}.amount;;
    }
    dimension_group: close{
      type: time
      timeframes: [date, week, month]
      convert_tz: no
      sql: ${TABLE}.close_date;;
    }
    dimension: created_by_id{
      hidden: yes
      sql: ${TABLE}.created_by_id;;
    }
    dimension_group: created{
      type: time
      timeframes: [time, date, week, month]
      sql: ${TABLE}.createddate;;
    }
    dimension: expected_revenue{
      type: number
      sql: ${TABLE}.expected_revenue;;
    }
    dimension: forecast_category{
      type: string
      sql: ${TABLE}.forecast_category;;
    }
    dimension: is_deleted{
      type: yesno
      sql: ${TABLE}.is_deleted;;
    }
    dimension: opportunityid{
      type: string
      hidden: yes
      sql: ${TABLE}.opportunityid;;
    }
    dimension: probability{
      type: number
      sql: ${TABLE}.probability;;
    }
    dimension: stage_name{
      type: string
      sql: ${TABLE}.stage_name;;
    }
    dimension_group: system_modstamp{
      type: time
      timeframes: [time, date, week, month]
      sql: ${TABLE}.system_modstamp;;
    }
# measures #

    measure: count{
      type: count
      drill_fields: [id, stage_name]
    }
  }
  view: opportunity_facts{
    derived_table:{
      sql:
      select account_id
        , sum(case
                when stage_name = 'Closed Won'
                then 1
                else 0
              end) as lifetime_opportunities_won
        , sum(case
                when stage_name = 'Closed Won'
                then acv
                else 0
              end) as lifetime_acv
      from salesforce.sf_opportunity
      group by 1;;


        sql_trigger_value: select current_date;;
      }

# dimensions #

      dimension: account_id{
        type: string
        primary_key: yes
        hidden: yes
        sql: ${TABLE}.account_id;;
      }
      dimension: lifetime_opportunities_won{
        type: number
        sql: ${TABLE}.lifetime_opportunities_won;;
      }
      dimension: lifetime_acv{
        label: "Lifetime ACV"
        type: number
        sql: ${TABLE}.lifetime_acv;;
      }
    }
    view: opportunity_history{
      extends: [_opportunity_history]}
    view: opportunity{
      extends: [_opportunity]}
