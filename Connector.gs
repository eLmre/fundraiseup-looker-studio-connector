// Initialize the Community Connector using the DataStudioApp service
let communityConnector = DataStudioApp.createCommunityConnector();

// Define the schema for the data structure of the connector
let schema = [
    {name: 'id', label: 'Donation ID', description: 'Unique identifier for the object.', dataType: 'STRING', isDefault: true, semantics: {conceptType: 'DIMENSION'}},
    {name: 'created_at', label: 'Created At', description: 'The time at which the object was created.', dataType: 'STRING', semantics: {conceptType: 'DIMENSION', 'semanticGroup': 'DATETIME', semanticType: 'YEAR_MONTH_DAY_SECOND'}},
    {name: 'comment', label: 'Comment', description: 'Nullable string', group: '', dataType: 'STRING', semantics: {conceptType: 'DIMENSION'}},
    {name: 'account_id', label: 'Account ID', description: 'Unique organization ID.', group: 'Account', dataType: 'STRING', semantics: {conceptType: 'DIMENSION'}},
    {name: 'account_code', label: 'Account Code', description: 'Organization code.', group: 'Account', dataType: 'STRING', semantics: {conceptType: 'DIMENSION'}},
    {name: 'account_name', label: 'Account Name', description: 'Organization name.', group: 'Account', dataType: 'STRING', semantics: {conceptType: 'DIMENSION'}},
    {name: 'anonymous', label: 'Anonymous', description: 'Anonymous donation.', dataType: 'BOOLEAN', semantics: {conceptType: 'DIMENSION'}},
    {name: 'livemode', label: 'Livemode', description: 'Test mode indicator. true for live mode donations, false for test mode donations.', dataType: 'BOOLEAN', semantics: {conceptType: 'DIMENSION'}},
    {name: 'status', label: 'Status', description: 'Can be: succeeded, scheduled, pending, retrying, refunded, or failed.', dataType: 'STRING', semantics: {conceptType: 'DIMENSION'}},
    {name: 'amount', label: 'Amount', description: 'Amount in donation currency.', dataType: 'NUMBER', semantics: {conceptType: 'METRIC', semanticGroup: 'CURRENCY'}},
    {name: 'amount_in_default_currency', label: 'Amount In Default Currency', description: 'Amount in the organization default currency at the time of the donation.', dataType: 'NUMBER', semantics: {conceptType: 'METRIC', semanticGroup: 'CURRENCY'}},
    {name: 'amount_before_fees_covered', label: 'Amount Before Fees Covered', description: 'Amount before fees covered by supporter.', dataType: 'NUMBER', semantics: {conceptType: 'METRIC', semanticGroup: 'CURRENCY'}},
    {name: 'amount_before_fees_covered_in_default_currency', label: 'Amount Before Fees Covered In Default Currency', description: 'Amount before fees covered by supporter in organization default currency at the time of the donation.', dataType: 'NUMBER', semantics: {conceptType: 'METRIC', semanticGroup: 'CURRENCY'}},
    {name: 'campaign_id', label: 'Campaign ID', description: 'Unique campaign identifier.', group: 'Campaign', dataType: 'STRING', semantics: {conceptType: 'DIMENSION'}},
    {name: 'campaign_code', label: 'Campaign Code', description: 'Campaign Code', group: 'Campaign', dataType: 'STRING', semantics: {conceptType: 'DIMENSION'}},
    {name: 'campaign_name', label: 'Campaign Name', description: 'Campaign Name', group: 'Campaign', dataType: 'STRING', semantics: {conceptType: 'DIMENSION'}},
    {name: 'designation_id', label: 'Designation', description: 'Unique designation identifier.', group: 'Designation', dataType: 'STRING', semantics: {conceptType: 'DIMENSION'}},
    {name: 'designation_code', label: 'Designation Code', description: '', group: 'Designation', dataType: 'STRING', semantics: {conceptType: 'DIMENSION'}},
    {name: 'designation_name', label: 'Designation Name', description: '', group: 'Designation', dataType: 'STRING', semantics: {conceptType: 'DIMENSION'}},
    {name: 'element_id', label: 'Element ID', description: 'Unique element identifier.', group: 'Element', dataType: 'STRING', semantics: {conceptType: 'DIMENSION'}},
    {name: 'element_type', label: 'Element Type', description: '', group: 'Element', dataType: 'STRING', semantics: {conceptType: 'DIMENSION'}},
    {name: 'element_name', label: 'Element Name', description: '', group: 'Element', dataType: 'STRING', semantics: {conceptType: 'DIMENSION'}},
    {name: 'currency', label: 'Currency', description: 'Three-letter ISO currency code, lowercase.', group: '', dataType: 'STRING', semantics: {semanticType: 'TEXT'}},
    {name: 'installment', label: 'Installment', description: 'null for one-time donations. 1+ for donations with recurring plan.', group: 'Recurring Plan', dataType: 'NUMBER', semantics: {conceptType: 'METRIC'}},
    {name: 'recurring_plan_id', label: 'Recurring Plan ID', description: 'Unique recurring identifier.', group: 'Recurring Plan', dataType: 'STRING', semantics: {conceptType: 'DIMENSION'}},
    {name: 'recurring_plan_status', label: 'Recurring Plan Status', description: 'Can be: active, scheduled, paused, retrying, completed, failed, or canceled', group: 'Recurring Plan', dataType: 'STRING', semantics: {conceptType: 'METRIC'}},
    {name: 'recurring_plan_frequency', label: 'Recurring Plan Frequency', description: 'Can be: daily, weekly, biweekly, every4weeks, monthly, bimonthly, quarterly, semiannual, or annual', group: 'Recurring Plan', dataType: 'STRING', semantics: {conceptType: 'METRIC'}},
    {name: 'recurring_plan_ended_at', label: 'Recurring Plan Ended at', description: 'Date on which the recurring plan ended.', group: 'Recurring Plan', dataType: 'STRING', semantics: {'semanticGroup': 'DATETIME', semanticType: 'YEAR_MONTH_DAY_SECOND'}},
    {name: 'succeeded_at', label: 'Succeeded at', description: 'The time at which the donation succeeded.', group: '', dataType: 'STRING', semantics: {'semanticGroup': 'DATETIME', semanticType: 'YEAR_MONTH_DAY_SECOND'}},
    {name: 'next_installment_at', label: 'Next Installment at', description: 'Timestamp of the next installment.', group: 'Recurring Plan', dataType: 'STRING', semantics: {'semanticGroup': 'DATETIME', semanticType: 'YEAR_MONTH_DAY_SECOND'}},
    {name: 'refunded_at', label: 'Refunded at', description: 'The time at which the donation was refunded.', group: '', dataType: 'STRING', semantics: {'semanticGroup': 'DATETIME', semanticType: 'YEAR_MONTH_DAY_SECOND'}},
    {name: 'failed_at', label: 'Failed at', description: 'The time at which the donation failed.', group: '', dataType: 'STRING', semantics: {'semanticGroup': 'DATETIME', semanticType: 'YEAR_MONTH_DAY_SECOND'}},
    {name: 'fundraiser_id', label: 'Fundraiser ID', description: 'Unique fundraiser ID.', group: 'Fundraiser', dataType: 'STRING', semantics: {conceptType: 'DIMENSION'}},
    {name: 'fundraiser_name', label: 'Fundraiser Name', description: 'Fundraiser name', group: 'Fundraiser', dataType: 'STRING', semantics: {conceptType: 'DIMENSION'}},
    {name: 'on_behalf_of', label: 'On Behalf Of', description: '', group: '', dataType: 'STRING', semantics: {conceptType: 'DIMENSION'}},
    {name: 'platform_fee_amount', label: 'Platform Fee Amount', description: 'Amount in donation currency', group: 'Platform Fee', dataType: 'NUMBER', semantics: {'semanticGroup': 'CURRENCY'}},
    {name: 'platform_fee_amount_in_default_currency', label: 'Platform Fee Amount in Default Currency', description: 'Amount in organization\’s default currency, at donation time', group: 'Platform Fee', dataType: 'NUMBER', semantics: {'semanticGroup': 'CURRENCY'}},
    {name: 'platform_fee_currency', label: 'Platform Fee Currency', description: 'Three-letter ISO currency code, lowercase', group: 'Platform Fee', dataType: 'STRING', semantics: {'semanticGroup': 'CURRENCY'}},
    {name: 'processing_fee_amount', label: 'Processing Fee Amount', description: 'Amount in donation currency', group: 'Processing Fee', dataType: 'NUMBER', semantics: {'semanticGroup': 'CURRENCY'}},
    {name: 'processing_fee_amount_in_default_currency', label: 'Processing Fee in Default Currency', description: 'Amount in organization\’s default currency, at donation time', group: 'Processing Fee', dataType: 'NUMBER', semantics: {'semanticGroup': 'CURRENCY'}},
    {name: 'processing_fee_currency', label: 'Processing Fee Currency', description: 'Three-letter ISO currency code, lowercase', group: 'Processing Fee', dataType: 'STRING'},
    {name: 'payment_id', label: 'Payment ID', description: 'For PayPal: Capture ID. For Stripe: Charge ID or Payment Intent ID. For other processors: payment ID.', group: 'Payment', dataType: 'STRING', semantics: {conceptType: 'DIMENSION'}},
    {name: 'payment_email', label: 'Payment Email', description: 'Email from payment method. Example: PayPal email.', group: 'Payment', dataType: 'STRING', semantics: {conceptType: 'DIMENSION'}},
    {name: 'payment_error_message', label: 'Payment Error Message', description: 'Returns the latest payment error message only if the status is failed or retrying. Otherwise, returns null.', group: 'Payment', dataType: 'STRING', semantics: {conceptType: 'METRIC'}},
    {name: 'payment_method', label: 'Payment Method', description: 'Payment method, in lowercase underscore. Examples: ach, apple_pay, google_pay, click_to_pay, paypal, venmo, becs_direct_debit, bacs_direct_debit, credit_card, pad, sepa_direct_debit, ideal, crypto, stock', group: 'Payment', dataType: 'STRING', semantics: {conceptType: 'DIMENSION'}},
    {name: 'payment_processor', label: 'Payment Processor', description: 'Payment processor, in lowercase. Examples: stripe, paypal, gemini, manual_brokerage, coinbase_commerce (deprecated)', group: 'Payment', dataType: 'STRING'},
    {name: 'payment_credit_card_exp_month', label: 'Payment CC Exp Month', description: 'Two-symbol string. Examples: 01, 12', group: 'Payment', dataType: 'STRING', semantics: {'semanticGroup': 'DATETIME', semanticType: 'MONTH'}},
    {name: 'payment_credit_card_exp_year', label: 'Payment CC Exp Year', description: 'Four-symbol string. Example: 2024', group: 'Payment', dataType: 'STRING', semantics: {'semanticGroup': 'DATETIME', semanticType: 'YEAR'}},
    {name: 'payment_credit_card_last4', label: 'Payment CC Last 4', description: 'Last four digits of the credit card number.', group: 'Payment', dataType: 'STRING'},
    {name: 'payout_amount', label: 'Payout Amount', description: 'Amount in donation\’s currency.', group: 'Payout', dataType: 'NUMBER', semantics: {conceptType: 'METRIC', semanticGroup: 'CURRENCY'}},
    {name: 'payout_amount_in_default_currency', label: 'Amount in organization\’s default currency, at donation time.', description: '', group: 'Payout', dataType: 'NUMBER', semantics: {conceptType: 'METRIC', semanticGroup: 'CURRENCY'}},
    {name: 'payout_currency', label: 'Payout Currency', description: 'Three-letter ISO currency code, lowercase', group: 'Payout', dataType: 'STRING'},
    {name: 'supporter_id', label: 'Supporter ID', description: 'Unique supporter ID', group: 'Supporter', dataType: 'STRING'},
    {name: 'supporter_email', label: 'Supporter Email', description: '', group: 'Supporter', dataType: 'STRING'},
    {name: 'supporter_first_name', label: 'Supporter First Name', description: '', group: 'Supporter', dataType: 'STRING'},
    {name: 'supporter_last_name', label: 'Supporter Last Name', description: '', group: 'Supporter', dataType: 'STRING'},
    {name: 'supporter_title', label: 'Supporter Title', description: 'Returns null if the title is empty. Title options include values like mr, mrs, etc', group: 'Supporter', dataType: 'STRING'},
    {name: 'supporter_phone', label: 'Supporter Phone', description: '', group: 'Supporter', dataType: 'STRING'},
    {name: 'supporter_employer_name', label: 'Supporter Employer Name ', description: '', group: 'Supporter', dataType: 'STRING'},
    {name: 'supporter_address_country', label: 'Supporter Address Country', description: 'Two-letter country code, lowercase.', group: 'Supporter', dataType: 'STRING', semantics: {'semanticGroup': 'GEO', semanticType: 'COUNTRY_CODE'}},
    {name: 'supporter_address_region', label: 'Supporter Address Region', description: '', group: 'Supporter', dataType: 'STRING', semantics: {'semanticGroup': 'GEO', semanticType: 'REGION_CODE'}},
    {name: 'supporter_address_city', label: 'Supporter Address City', description: '', group: 'Supporter', dataType: 'STRING', semantics: {'semanticGroup': 'GEO', semanticType: 'CITY'} },
    {name: 'supporter_address_line1', label: 'Supporter Address Line1', description: '', group: 'Supporter', dataType: 'STRING'},
    {name: 'supporter_address_line2', label: 'Supporter Address Line2', description: '', group: 'Supporter', dataType: 'STRING'},
    {name: 'supporter_address_postal_code', label: 'Supporter Address Postal Code', description: 'Postal/ZIP code', group: 'Supporter', dataType: 'STRING'},
    {name: 'supporter_covered_fee', label: 'Supporter Covered Fee', description: 'Fees covered by supporter. Decimal string.', group: 'Supporter', dataType: 'NUMBER', semantics: {'semanticGroup': 'CURRENCY'}},
    {name: 'source', label: 'Source', description: 'Can be: website, campaign_page, virtual_terminal, recurring_migration, api', group: '', dataType: 'STRING'},
    {name: 'tribute_id', label: 'Tribute ID', description: '', group: 'Tribute', dataType: 'STRING'},
    {name: 'tribute_type', label: 'Tribute Type', description: 'Can be: in_honor or in_memory', group: 'Tribute', dataType: 'STRING'},
    {name: 'tribute_honoree', label: 'Tribute Honoree', description: '', group: 'Tribute', dataType: 'STRING'},
    {name: 'tribute_sharing_type', label: 'Tribute Sharing Type', description: 'Sharing destination field name. Can be: email or address', group: 'Tribute', dataType: 'STRING'},
    {name: 'tribute_sharing_from', label: 'Tribute Sharing From', description: '', group: 'Tribute', dataType: 'STRING'},
    {name: 'tribute_sharing_message', label: 'Tribute Sharing Message', description: '', group: 'Tribute', dataType: 'STRING'},
    {name: 'tribute_sharing_recipient_email', label: 'Tribute Sharing Recipient Email', description: 'Email address for the email sharing type.', group: 'Tribute', dataType: 'STRING'},
    {name: 'tribute_sharing_recipient_address_country', label: 'Tribute Sharing Recipient Address Country', description: 'Two-letter country code, lowercase.', group: 'Tribute', dataType: 'STRING', semantics: {'semanticGroup': 'GEO', semanticType: 'COUNTRY'}},
    {name: 'tribute_sharing_recipient_address_region', label: 'Tribute Sharing Recipient Address Region', description: 'Region/state/province.', group: 'Tribute', dataType: 'STRING', semantics: {'semanticGroup': 'GEO', semanticType: 'REGION_CODE'}},
    {name: 'tribute_sharing_recipient_address_city', label: 'Tribute Sharing Recipient Address City', description: '', group: 'Tribute', dataType: 'STRING', semantics: {'semanticGroup': 'GEO', semanticType: 'CITY'}},
    {name: 'tribute_sharing_recipient_address_line1', label: 'Tribute Sharing Recipient Address Line1', description: '', group: 'Tribute', dataType: 'STRING'},
    {name: 'tribute_sharing_recipient_address_line2', label: 'Tribute Sharing Recipient Address Line2', description: '', group: 'Tribute', dataType: 'STRING'},
    {name: 'tribute_sharing_recipient_address_postal_code', label: 'Tribute Sharing Recipient Address Postal Code', description: '', group: 'Tribute', dataType: 'STRING'},
    {name: 'tribute_sharing_recipient_title', label: 'Tribute Sharing Recipient Title', description: '', group: 'Tribute', dataType: 'STRING'},
    {name: 'tribute_sharing_recipient_first_name', label: 'Tribute Sharing Recipient First Name', description: '', group: 'Tribute', dataType: 'STRING'},
    {name: 'tribute_sharing_recipient_last_name', label: 'Tribute Sharing Recipient Last Name', description: '', group: 'Tribute', dataType: 'STRING'},
    {name: 'device_browser', label: 'Device Browser', description: '', group: 'Device', dataType: 'STRING'},
    {name: 'device_os', label: 'Device OS', description: '', group: 'Device', dataType: 'STRING'},
    {name: 'device_type', label: 'Device Type', description: '', group: 'Device', dataType: 'STRING'},
    {name: 'device_ip_address', label: 'Device IP', description: '', group: 'Device', dataType: 'STRING'},
    {name: 'device_ip_country_name', label: 'Device Country', description: '', group: 'Device', dataType: 'STRING', semantics: {'semanticGroup': 'GEO', semanticType: 'COUNTRY'}},
    {name: 'device_ip_region', label: 'Device Region', description: '', group: 'Device', dataType: 'STRING', semantics: {'semanticGroup': 'GEO', semanticType: 'REGION'}},
    {name: 'device_ip_city', label: 'Device City', description: '', group: 'Device', dataType: 'STRING', semantics: {'semanticGroup': 'GEO', semanticType: 'CITY'}},
    {name: 'utm_source', label: 'UTM Source', description: '', group: 'UTM', dataType: 'STRING'},
    {name: 'utm_campaign', label: 'UTM Campaign', description: '', group: 'UTM', dataType: 'STRING'},
    {name: 'utm_content', label: 'UTM Content', description: '', group: 'UTM', dataType: 'STRING'},
    {name: 'utm_medium', label: 'UTM Medium', description: '', group: 'UTM', dataType: 'STRING'},
    {name: 'utm_term', label: 'UTM Term', description: '', group: 'UTM', dataType: 'STRING'},
    {name: 'url', label: 'URL', description: 'URL, from which the donation was made.', group: '', dataType: 'STRING', semantics: {'semanticGroup': 'GEO', semanticType: 'URL'}},
    // custom
    {name: 'frequency', label: 'Frequency', description: 'Can be: One-time, Recurring, First installment', group: '', dataType: 'NUMBER', formula: 'CASE WHEN installment IS NULL THEN "One-time" WHEN installment >= 1 THEN "Recurring" WHEN installment = 1 THEN "First installment" WHEN recurring_plan_frequency = "daily" THEN "Daily" WHEN recurring_plan_frequency = "weekly" THEN "Weekly" WHEN recurring_plan_frequency = "biweekly" THEN "Biweekly" WHEN recurring_plan_frequency = "every4weeks" THEN "Every 4 weeks" WHEN recurring_plan_frequency = "monthly" THEN "Monthly" WHEN recurring_plan_frequency = "bimonthly" THEN "Bimonthly" WHEN recurring_plan_frequency = "quarterly" THEN "Quarterly" WHEN recurring_plan_frequency = "semiannual" THEN "Semiannually" WHEN recurring_plan_frequency = "annual" THEN "Annually" END'},
    {name: 'device_type_mobile', label: 'Device Type Mobile', description: '', group: '', dataType: 'NUMBER', formula: 'CASE WHEN device_type = "mobile" THEN 1 ELSE 0 END', spreadsheet_formula: '=SWITCH(${device_type}, "mobile", 1, 0)'},
];

// Return the defined schema to Data Studio
function getSchema(request) {
  Logger.log('Run getSchema()');
  let data = fetchDataApi(request);
  let customFieldsSchema = buildCustomFieldsSchema(data);
  schema.push(...customFieldsSchema);
  return { schema: schema };
};

// Define the configuration settings for the connector, including user input fields
function getConfig(request) {
  let config = communityConnector.getConfig();

  config.newInfo()
      .setId('instructions')
      .setText('To use the Fundraise Up API, you need an API key. Go to Dashboard > Settings > API keys and click Create API key. You can create multiple keys for the same account.');

  config.newTextInput()
      .setId('api_key')
      .setName('API Key')
      .setHelpText('. . .')
      .setPlaceholder('')
      .setAllowOverride(true);

  /**
   * FundraiseUp REST Api does not support timestamp queries.
   * Also, to collect all possible custom fields keys for the schema,
   * we collect the maximum possible amount of rows in one api request.
   */
  config.setDateRangeRequired(false);

  return config.build();
}

function fetchDataApi(request, skip_cache = false, write_cache = true) {
  Logger.log('Run fetchDataApi()');

  let scriptProperties = PropertiesService.getScriptProperties();
  let api_key = null;

  // We should be able to fetch API without the request variable.
  // Use PropertiesService to save latest used key.
  if ( request && 'api_key' in request && request.api_key ) {
    // This is a temporary key. Do not save in scriptProperties
    api_key = request.api_key;
  } else if( request && 'configParams' in request ) {
    api_key = request.configParams.api_key;
    scriptProperties.setProperty('api_key', api_key);
  } else {
    api_key = scriptProperties.getProperty('api_key');
  }

  if( !api_key ) {
    throw new Error( "No API key defined." );
  }

  let data = null;

  let cache = new DataCache( CacheService.getUserCache(), api_key );

  console.log('fetchDataApi() - Skip cache? ', (skip_cache)?'Yes':'No');

  if( !skip_cache ) {
    data = getCache(cache);
  }

  if ( data !== null && ('has_more' in data && !data.has_more) ) {
    console.log('fetchDataApi() - Returning data from cache.');
    return data;
  }

  console.log( 'fetchDataApi() - Skip the cache...' );

  // Gets a script lock before sending API reqiests.
  var lock = LockService.getScriptLock();
  // Waits for up to 60 seconds for other reqiests to finish.
  try {
    // TODO: This time can be adaptive
    lock.waitLock(30000);

    // The lock was released during 30 seconds await, trying to check the cache again.
    console.log( 'fetchDataApi() - waitLock successfully passed...' );

    if( !skip_cache ) {
      data = getCache(cache);
    }

    if ( data !== null && ('has_more' in data && !data.has_more) ) {
      return data;
    }

  } catch (e) {
    Logger.log('fetchDataApi() - Could not obtain lock after 30 seconds.');
    lock.releaseLock();
    throw('Error: ' + e);
  }

  data = data || { data: [], has_more: true };

  let headers = { Authorization: 'Bearer ' + api_key };
  let url_raw = 'https://api.fundraiseup.com/v1/donations?limit=100'; // max 100
  let url = url_raw;

  let starting_after = null;

  if ( request !== undefined && request.starting_after !== undefined ) {
    starting_after = request.starting_after;
  } else if ('data' in data && data.data.length ) {
    starting_after = data.data[data.data.length - 1].id
  }

  if( starting_after ) {
    url += '&starting_after='+ starting_after;
  }

  let ending_before = null;

  if( request !== undefined && request.ending_before !== undefined ) {
    url += '&ending_before='+ request.ending_before;
  }

  let request_count = 0;
  let timer_second = Date.now();
  let timer_minute = Date.now();
  let overslept = 0;
  let max_execution_timer = Date.now();
  //let max_execution_limit_free = 355000; // 5,9 min - regular account
  let max_execution_limit_paid = 1770000; // 29.5 mins - paid account

  do {
    request_count++;

     /*
     * 6 mins. per execution for free account and 30 mins on paid google cloud account.
     * Since this trigger was created from an add-on it cannot be fired in intervals less than one hour.
     * The function will return the resulting value for the first run.
     * We have the ability to update cached data in the background proccess, get this data later is better than just never.
     */
    if( Date.now() - max_execution_timer > max_execution_limit_paid ) {
      console.log('fetchDataApi() - Max execution time has been exceeded on request #'+ request_count );
      Async.apply('fetchDataApi', ['', false], {after:1000});
      break;
    }

    // Rate limit: 8 requests per second
    var sleep_second = 1000 - ( Date.now() - timer_second );
    if( request_count % 8 === 0 && sleep_second > 0 ) {
      // we made 8 requests in less than 1 second - need some sleep
      overslept = overslept + sleep_second; // save how much we slept
      Utilities.sleep( sleep_second ); // wait for the second to end
      timer_second = Date.now(); // reset timer
    }

    // Rate limit: 128 requests per minute
    // If we were already asleep before this moment, subtract this time
    var sleep_minute = 60000 - overslept - ( Date.now() - timer_minute );
    if( request_count % 128 === 0 && sleep_minute > 0 ) {
      // we made 128 requests in less than 1 minute - need some sleep
      Utilities.sleep( sleep_minute ); // wait for the minute to end
      timer_minute = Date.now(); // reset timer
      overslept = 0;
    }

    console.log('fetchDataApi() - Page: '+ request_count +' URL:', url);

    let parsedResponse = requestURL( url, { headers: headers } );

    if(!parsedResponse) {
      // Failure. How about try again after 5 minutes?
      var request = {};

      if( starting_after ) {
          request['starting_after'] = starting_after;
      }

      if( ending_before ) {
          request['ending_before'] = ending_before;
      }

      Async.apply('fetchDataApi', [request, skip_cache, write_cache], {after: 5 * 60 * 1000 });

      break;
    }

    data.data.push(...parsedResponse.data);
    data.has_more = parsedResponse.has_more;

    if (!parsedResponse.has_more) {
      break;
    }

    url = url_raw;

    if( starting_after || !ending_before ) {
        url += '&starting_after='+ parsedResponse.data[parsedResponse.data.length - 1].id;
    }

    // Stop spam, no rush
    Utilities.sleep(1000);

  } while (data.has_more);

  console.log('fetchDataApi() - Found '+ data.data.length +' donations');

  if( data.data.length && write_cache ) {
    setCache(data, cache);

    // Refresh cache after 5 hours (should be less than 6 hours because of cache duration limit)
    Async.apply('fetchDataApi', ['', true, true], {after: 5 * 60 * 60 * 1000 });
  }

  // Releases the lock so that other processes can continue.
  lock.releaseLock();

  return data;
}

function buildCustomFieldsSchema(data) {
  if ( data === undefined || data.data === undefined || data.data.length === 0 ) {
    return [];
  }

  let customFieldsSchema = [];

  data.data.forEach(function(donation) {
    if( !donation.custom_fields.length ) {
      return;
    }

    donation.custom_fields.forEach(function(custom_field) {
      // Shema field name should be unique
      let field_name = 'custom_fields_'+ custom_field.name;

      // Check for duplicate inside current request
      let duplicate_check = customFieldsSchema.find(({ name }) => name === field_name );

      // Check if schema already has this custom field
      let duplicate_check_schema = schema.find(({ name }) => name === field_name );

      if ( typeof duplicate_check == 'undefined' && typeof duplicate_check_schema == 'undefined' ) {

        // duplicate not found - add custom field to schema
        var dataType = null;
        switch ( typeof custom_field.value ) {
          case 'number':
            dataType = 'NUMBER';
            break;
          case 'boolean':
            dataType = 'BOOLEAN';
            break;
          default:
            dataType = 'STRING';
        }

        customFieldsSchema.push( {name: field_name, label: 'Custom Field ' + convertToTitleCase(custom_field.name), description: '', group: 'Custom Fields', dataType: dataType, semantics: {isReaggregatable: true}} );

      }
    });
  });

  return customFieldsSchema;
}

function buildRows(request, data, type = '') {
  Logger.log('Run buildRows()');

  let rows = [];
  let row_number = request.last_row + 1 || 2;

  data.data.forEach(function(donation) {
    let row = [];
    let underscoredObj = underscoreNotate(donation); // read nested objects as first level

    request.fields.forEach(function(field) {

        if( type === 'spreadsheet' && field.hasOwnProperty('spreadsheet_formula') && field.spreadsheet_formula ) {

          // parse formula then add as formula string
          row.push( parseFormula(field, request, row_number) );

        } else if ( field.name in donation ) {

          // add from original response object
          row.push(validateValue(field, donation[field.name], type));

        } else if ( field.name in underscoredObj ) {

          // looks like it's nested object key
          // add from underscored object
          row.push(validateValue(field, underscoredObj[field.name], type));

        } else if ( donation.custom_fields.length && donation.custom_fields.find(({ name }) => name === field.name.split("custom_fields_").pop()) ) {

          // Match found in custom fields
          let donation_custom_field = donation.custom_fields.find(({ name }) => name === field.name.split("custom_fields_").pop());
          row.push(donation_custom_field.value);

        } else {

          row.push('');

        }

    });

    row_number++;

    if( type === 'spreadsheet' ) {
      rows.push(row);
    } else {
      rows.push({ values: row });
    }

  });

  console.log( 'Total donations:', rows.length );

  return rows;
}

function getData(request) {
  Logger.log('Run getData()');

  let data = fetchDataApi(request);
  let rows = buildRows(request, data);
  let customFieldsSchema = buildCustomFieldsSchema(data);

  schema.push(...customFieldsSchema);

  // Get the fields requested by Looker Studio
  let dataSchema = [];
  request.fields.forEach(function (field) {
      for (const element of schema) {
          if (element.name == field.name) {
              dataSchema.push(element);
              break;
          }
      }
  });

  return {
      schema: dataSchema,
      rows: rows
  };
}

// Specify the authentication type for the connector
function getAuthType() {
    // This connector does not require authentication
    return { type: 'NONE' };
}

// Check if the current user has administrative privileges
function isAdminUser() {
    // For this example, all users are treated as admin users
    return true;
}

function underscoreNotate(obj, target, prefix) {
  target = target || {},
  prefix = prefix || "";

  Object.keys(obj).forEach(function(key) {
    if ( typeof(obj[key]) === "object" && obj[key] !== null ) {
      underscoreNotate(obj[key],target,prefix + key + "_");
    } else {
      return target[prefix + key] = obj[key];
    }
  });

  return target;
}

function getCache(cache) {
  var data = null;
  console.log('Trying to fetch from cache...');
  try {
    var cachedString = cache.get();
    data = JSON.parse(cachedString);
    console.log('Fetched succesfully from cache', data.data.length + ' donations.');
  } catch (e) {
    console.log('Error when fetching from cache:', e);
  }

  return data;
}

function setCache(data, cache) {
  console.log('Setting data to cache...');
  try {
    cache.set(JSON.stringify(data));
  } catch (e) {
    console.log('Error when storing in cache', e);
  }
}

function convertToTitleCase(str) {
  if (!str) {
      return ""
  }

  const exceptions = ['of', 'the', 'and'];
  return str.replace(/[^a-zA-Z0-9-. ]/g, ' ').toLowerCase().split(' ').map((word, i) => {
            return exceptions.includes(word) && i != 0 ? word : word.charAt(0).toUpperCase().concat(word.substr(1));
        }).join(' ');
}

/**
 *  Converts date strings to YYYYMMDDHH:mm:ss
 */
function convertDate(value) {
  var date = new Date(value);
  return (
    date.getUTCFullYear() +
    ('0' + (date.getUTCMonth() + 1)).slice(-2) +
    ('0' + date.getUTCDate()).slice(-2) +
    ('0' + date.getUTCHours()).slice(-2)
  );
}

function fmt(date, format = 'YYYYMMDDhhmmss') {
  const pad2 = (n) => n.toString().padStart(2, '0');

  const map = {
    YYYY: date.getFullYear(),
    MM: pad2(date.getMonth() + 1),
    DD: pad2(date.getDate()),
    hh: pad2(date.getHours()),
    mm: pad2(date.getMinutes()),
    ss: pad2(date.getSeconds()),
  };

  return Object.entries(map).reduce((prev, entry) => prev.replace(...entry), format);
}

/**
 * Validates the row values. Only numbers, boolean, date and strings are allowed
 */
function validateValue(field, value, type) {

  // For spreadsheet save date without formating
  if( ['created_at', 'succeeded_at', 'failed_at', 'refunded_at', 'recurring_plan_ended_at'].includes(field.name) && value !== null ) {
    var date = new Date(value);

    if( type === 'spreadsheet' ) {
      value = date.toISOString();
    } else {
      value = fmt(date);
    }

    return value;
  }

  switch (typeof value) {
    case 'string':
    case 'number':
    case 'boolean':
      return value;
    case 'object':
      if( Object.is(value, null) ) {
        return '';
      } else {
        return JSON.stringify(value);
      }
  }

  return '';
}

function requestURL( url, args ) {
    if ( !url || !args ) {
      return false;
    }

    var maxAttempts = 4;
    var attempts = 0;
    var response;

    while (attempts < maxAttempts) {
        try {
            response = UrlFetchApp.fetch(url, args);
            if (response.getResponseCode() === 200) {
                return JSON.parse(response.getContentText());
            } else {
                Logger.log("Failure, attempt " + (attempts + 1));
            }
        } catch (e) {
            Logger.log("Error on attempt " + (attempts + 1) + ": " + e.message);
        }
        attempts++;
        if (attempts < maxAttempts) {
            var wait_time = 5000 * attempts;
            Logger.log("Waiting " + wait_time + " seconds");
            Utilities.sleep( wait_time ); // make a pause then try again
        }
    }

    return false;
}

function parseFormula( field, request, row_number ) {
  //console.log( field );
  //console.log( request );

  if ( !field.hasOwnProperty('spreadsheet_formula') || !field['spreadsheet_formula'] || !request.hasOwnProperty('fields') || !request['fields'].length ) {
    return '';
  }

  var formula_string = field.spreadsheet_formula;
  var variables_from_formula_string = [];

  // Regular expression to match ${variable_name}
  var regex = /\$\{([^}]+)\}/g;
  var match;

  // Loop through all matches and add them to the array
  while ((match = regex.exec(formula_string)) !== null) {
      variables_from_formula_string.push(match[1]);
  }

  for (let i = 0; i < variables_from_formula_string.length; i++) {
      let variable_field = request.fields.find(obj => obj.name === variables_from_formula_string[i]);

      if (variable_field.hasOwnProperty('letter') && variable_field['letter']) {
          let variable_value = variable_field['letter'] + row_number;

          // Replace the variable key in the formula_string with the variable_value
          formula_string = formula_string.replace(`\${${variables_from_formula_string[i]}}`, variable_value);
      }
  }

  return formula_string;
}


