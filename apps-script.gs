/**
 * Shop Pay Business Case Builder — Google Apps Script Backend
 * ───────────────────────────────────────────────────────────
 * SETUP (one-time, ~5 minutes):
 *
 * 1. Go to script.google.com → New project → name it "SP Pitch Builder API"
 * 2. Paste this entire file into Code.gs
 * 3. Left sidebar → Services (+) → Add "BigQuery API" → Add
 * 4. Click Deploy → New deployment
 *    - Type: Web App
 *    - Execute as: Me
 *    - Who has access: Anyone
 *    Click Deploy → copy the Web App URL
 * 5. Paste that URL into the SP Pitch Builder tool (the settings panel in the nav)
 *
 * The tool will now auto-fill all metrics when an AE enters a Shop ID.
 */

// ── ENTRY POINTS ──────────────────────────────────────────────────────────────

function doPost(e) {
  try {
    const params = JSON.parse(e.postData.contents);
    const shopId = parseInt(params.shopId);
    if (!shopId || isNaN(shopId)) {
      return jsonResponse({ error: 'Invalid Shop ID' });
    }
    return jsonResponse(fetchMerchantData(shopId));
  } catch (err) {
    return jsonResponse({ error: err.message });
  }
}

// Test via browser: ?shopId=55472324767
function doGet(e) {
  const shopId = parseInt(e.parameter.shopId || '0');
  if (!shopId) return jsonResponse({ error: 'Provide ?shopId=... in the URL' });
  return jsonResponse(fetchMerchantData(shopId));
}

function jsonResponse(data) {
  return ContentService
    .createTextOutput(JSON.stringify(data))
    .setMimeType(ContentService.MimeType.JSON);
}

// ── MAIN DATA FETCH ──────────────────────────────────────────────────────────

function fetchMerchantData(shopId) {
  const result = { shopId: shopId, fetchedAt: new Date().toISOString() };

  // Run all queries and merge results
  try { Object.assign(result, queryFunnel(shopId)); } catch(e) { result.funnelError = e.message; }
  try { Object.assign(result, queryShopPay(shopId)); } catch(e) { result.spError = e.message; }
  try { Object.assign(result, queryTransactions(shopId)); } catch(e) { result.txError = e.message; }

  // Derived: revenue uplift estimates (require AOV from transactions query)
  if (result.totalOrders && result.sessions && result.cartRatePct && result.aov) {
    const sessNoCart = result.sessions * (1 - result.cartRatePct / 100);
    const cartAdds   = result.totalOrders / (result.checkoutCompPct / 100) || result.cartAdds;
    const chkDropoffs = (cartAdds || 0) - result.totalOrders;
    result.revCart = Math.round(sessNoCart * 0.05 * result.aov);
    result.revChk  = Math.round(Math.max(chkDropoffs, 0) * 0.05 * result.aov);
  }

  return result;
}

// ── QUERY 1: PURCHASE FUNNEL ─────────────────────────────────────────────────
// Table: sdp-for-analysts-platform.growth_services_prod.purchase_funnel_new
// This is a pre-aggregated 90-day rolling snapshot, one row per device/landing_page_type combo.
// Columns: shop_id, device, landing_page_type, sessions, added_to_cart_count,
//          checkout_completed_count, add_to_cart_rate, checkout_completion_rate,
//          p_50_add_to_cart_rate, p_50_checkout_completion_rate

function queryFunnel(shopId) {
  const sql = `
    SELECT
      SUM(sessions)                                                               AS total_sessions,
      SUM(added_to_cart_count)                                                    AS total_cart_adds,
      SUM(checkout_completed_count)                                               AS total_orders,
      ROUND(SAFE_DIVIDE(SUM(added_to_cart_count), SUM(sessions)) * 100, 2)       AS cart_rate_pct,
      ROUND(SAFE_DIVIDE(SUM(checkout_completed_count),
            SUM(added_to_cart_count)) * 100, 2)                                  AS checkout_comp_pct,
      ROUND(MAX(p_50_add_to_cart_rate) * 100, 2)                                 AS benchmark_cart_pct,
      ROUND(MAX(p_50_checkout_completion_rate) * 100, 2)                         AS benchmark_chk_pct,
      ROUND(SAFE_DIVIDE(
        SUM(CASE WHEN LOWER(device) = 'mobile' THEN sessions ELSE 0 END),
        SUM(sessions)
      ) * 100, 1)                                                                 AS mobile_pct
    FROM \`sdp-for-analysts-platform.growth_services_prod.purchase_funnel_new\`
    WHERE shop_id = @shopId
  `;

  const rows = runQuery(sql, shopId, 'sdp-for-analysts-platform');
  if (!rows || rows.length === 0) return {};

  const f = rows[0].f;
  return {
    sessions:        safeNum(f[0].v),
    cartAdds:        safeNum(f[1].v),
    totalOrders:     safeNum(f[2].v),
    cartRatePct:     safeNum(f[3].v),   // e.g. 3.48
    checkoutCompPct: safeNum(f[4].v),   // e.g. 60.14
    benchmarkCartPct: safeNum(f[5].v),  // e.g. 5.42
    benchmarkChkPct:  safeNum(f[6].v),  // e.g. 40.10
    mobilePct:        safeNum(f[7].v),  // e.g. 77.7
  };
}

// ── QUERY 2: SHOP PAY ATTRIBUTES ─────────────────────────────────────────────
// Table: sdp-prd-commercial.intermediate.copilot_shop_pay_attributes
// Columns: shop_id, eligible_for_shop_pay, adopted_shop_pay,
//          shop_pay_last_activated_date, shop_pay_estimated_revenue_boost_usd_l30d,
//          shop_app_channel_installed

function queryShopPay(shopId) {
  const sql = `
    SELECT
      adopted_shop_pay,
      ROUND(shop_pay_estimated_revenue_boost_usd_l30d, 0)        AS sp_rev_boost_30d,
      ROUND(shop_pay_estimated_revenue_boost_usd_l30d * 3, 0)    AS sp_rev_boost_90d,
      shop_app_channel_installed,
      eligible_for_shop_pay,
      CAST(shop_pay_last_activated_date AS STRING)                AS sp_activated_date
    FROM \`sdp-prd-commercial.intermediate.copilot_shop_pay_attributes\`
    WHERE shop_id = @shopId
    LIMIT 1
  `;

  const rows = runQuery(sql, shopId, 'sdp-prd-commercial');
  if (!rows || rows.length === 0) return {};

  const f = rows[0].f;
  return {
    adoptedShopPay:     f[0].v === 'true',
    spRevBoost30d:      safeNum(f[1].v),
    spRevBoost90d:      safeNum(f[2].v),   // Maps to sp-rev field (90-day estimate)
    shopAppInstalled:   f[3].v === 'true',
    eligibleForShopPay: f[4].v === 'true',
    spActivatedDate:    f[5].v || null,
  };
}

// ── QUERY 3: TRANSACTION-LEVEL GATEWAY DATA (90d) ────────────────────────────
// Table: shopify-dw.money_products.order_transactions_payments_summary
// Key fields: shop_id, order_id, is_shopify_payments_gateway, card_wallet_type,
//             amount_local, buyer_experience_type, payment_gateway_integration_label,
//             order_transaction_created_at, order_transaction_kind, order_transaction_status

function queryTransactions(shopId) {
  const sql = `
    WITH txns AS (
      SELECT
        order_id,
        is_shopify_payments_gateway,
        card_wallet_type,
        COALESCE(amount_local, 0)            AS amount,
        buyer_experience_type,
        payment_gateway_integration_label,
        payment_gateway_integration_name,
        order_transaction_status
      FROM \`shopify-dw.money_products.order_transactions_payments_summary\`
      WHERE shop_id          = @shopId
        AND order_transaction_created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
        AND order_transaction_kind IN ('sale', 'capture')
        AND order_transaction_status = 'success'
        AND NOT COALESCE(is_test, FALSE)
        AND NOT COALESCE(is_pos, FALSE)
    ),
    totals AS (
      SELECT
        COUNT(DISTINCT order_id)                                                    AS total_orders,
        SUM(amount)                                                                 AS total_gmv,
        SAFE_DIVIDE(SUM(amount), COUNT(DISTINCT order_id))                          AS aov,

        -- % GMV via Shopify Payments gateway
        ROUND(SAFE_DIVIDE(
          SUM(CASE WHEN is_shopify_payments_gateway THEN amount ELSE 0 END),
          SUM(amount)
        ) * 100, 1)                                                                 AS sp_gmv_pct,

        -- % orders where buyer used Shop Pay wallet
        ROUND(SAFE_DIVIDE(
          COUNT(DISTINCT CASE WHEN card_wallet_type = 'shopify_pay' THEN order_id END),
          COUNT(DISTINCT order_id)
        ) * 100, 1)                                                                 AS sp_orders_pct,

        -- Has offsite (redirect) gateway used by buyers?
        MAX(CASE WHEN buyer_experience_type = 'offsite'
                  AND NOT COALESCE(is_shopify_payments_gateway, FALSE) THEN 1 ELSE 0 END) AS has_redirect_gw

      FROM txns
    ),
    -- 2nd largest gateway by GMV (non-SP)
    gateways AS (
      SELECT
        payment_gateway_integration_label,
        SUM(amount) AS gateway_gmv,
        MAX(buyer_experience_type) AS experience_type
      FROM txns
      WHERE NOT COALESCE(is_shopify_payments_gateway, FALSE)
      GROUP BY 1
      ORDER BY 2 DESC
      LIMIT 1
    )
    SELECT
      t.total_orders,
      ROUND(t.total_gmv, 0)     AS total_gmv,
      ROUND(t.aov, 0)           AS aov,
      t.sp_gmv_pct,
      t.sp_orders_pct,
      t.has_redirect_gw,
      g.payment_gateway_integration_label AS second_gateway
    FROM totals t
    LEFT JOIN gateways g ON TRUE
  `;

  const rows = runQuery(sql, shopId, 'shopify-dw');
  if (!rows || rows.length === 0) return {};

  const f = rows[0].f;
  return {
    txTotalOrders:  safeNum(f[0].v),
    totalGmv:       safeNum(f[1].v),
    aov:            safeNum(f[2].v),      // Average Order Value
    spGmvPct:       safeNum(f[3].v),      // Maps to sp-gmv field
    spOrdersPct:    safeNum(f[4].v),      // Maps to sp-orders field
    hasRedirectGw:  f[5].v === '1',       // Maps to redirect-gateway checkbox
    secondGateway:  f[6].v || null,       // Maps to gateway2 field
  };
}

// ── HELPERS ──────────────────────────────────────────────────────────────────

function runQuery(sql, shopId, billingProject) {
  const request = {
    query:       sql,
    useLegacySql: false,
    location:    'US',
    timeoutMs:   30000,
    queryParameters: [{
      name:           'shopId',
      parameterType:  { type: 'INT64' },
      parameterValue: { value: shopId.toString() }
    }]
  };

  const response = BigQuery.Jobs.query(request, billingProject);

  // If not complete yet, poll
  if (!response.jobComplete) {
    const jobId    = response.jobReference.jobId;
    let pollResult = BigQuery.Jobs.getQueryResults(billingProject, jobId, { timeoutMs: 25000 });
    let attempts   = 0;
    while (!pollResult.jobComplete && attempts < 10) {
      Utilities.sleep(2000);
      pollResult = BigQuery.Jobs.getQueryResults(billingProject, jobId, { timeoutMs: 10000 });
      attempts++;
    }
    return pollResult.rows || [];
  }

  return response.rows || [];
}

function safeNum(v) {
  if (v === null || v === undefined || v === '') return null;
  const n = parseFloat(v);
  return isNaN(n) ? null : n;
}
