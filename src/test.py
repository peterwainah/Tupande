 select contracts.reference as Contract_reference ,contracts.[status],contracts.[nominal_contract_value],contracts.[cumulative_amount_paid],leads.state,leads.county,
 DATEADD(day, 180, CONVERT(date, contracts.[start_date], 126)) as End_date,contract_offer.[name],CASE 
           WHEN name LIKE '%group%' THEN 'Group loan'
           WHEN name LIKE '%individual%' THEN 'Individual loan'
           WHEN name LIKE '%cash%' THEN 'Cash sale'
           ELSE 'Unknown'
       END AS loan_type,
	   CASE 
           WHEN loan_type = 'Group loan' THEN DATEADD(day, 30, End_date)
           WHEN loan_type = 'Paygo loan' THEN DATEADD(day, 30, End_date)
           WHEN loan_type = 'Individual loan' THEN DATEADD(day, 60, End_date)
           ELSE NULL
       END AS maturity_date

 from stg.contracts 
 left join [stg].[leads]  on leads.id=contracts.lead_id
 left join [stg].contract_offer on contract_offer.id=contracts.offer_id