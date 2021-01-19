const {BigQuery} = require('@google-cloud/bigquery');
const axios = require('axios');

// Create Active Campaign Client
const activeCampaign = axios.create({
    baseURL: process.env.ACTIVE_CAMPAIGN_URI,
    headers: {
        "Content-Type": "application/json",
        "Api-Token": process.env.ACTIVE_CAMPAIGN_API_KEY,
    }
});

// Create BigQuery Client
let bigquery;

if (process.env.NODE_ENV === 'development') {
  bigquery = new BigQuery({
    projectId: process.env.GOOGLE_PROJECT_ID,
    keyFilename: process.env.BIGQUERY_KEY_FILE_PATH
  });
} else {
  // keyfile not required within Google Cloud (if same account)
  bigquery = new BigQuery();
}

function insertRowsToGBQ(rows) {
  // Retrieve the dataset / table
  const dataset = bigquery.dataset(process.env.BIGQUERY_DATASET);
  const table = dataset.table(process.env.BIGQUERY_TABLE);

  // Insert Rows
  table.insert(rows, (error, apiResponse) => {
    if (error) console.log(JSON.stringify(error));
  });
}

async function getCampaigns () {
  try {
    let acResponse = await activeCampaign.get('/campaigns');
    let campaigns = acResponse.data.campaigns;

    return campaigns.map(mapCampaignToSchema);
  } catch (error) {
    console.log(error);
    return;
  }
}

function mapCampaignToSchema (campaign) {
  return {
    campaign_id: campaign.id,
    created_at: formatDate(new Date()),
    name: campaign.name,
    campaign_created: formatDate(new Date(campaign.cdate)),
    opens: parseInt(campaign.opens),
    clicks: parseInt(campaign.linkclicks),
    unique_opens: parseInt(campaign.uniqueopens),
    unique_clicks: parseInt(campaign.uniquelinkclicks),
    campaign_sent: formatDate(new Date(campaign.sdate)),
    sends: parseInt(campaign.send_amt),
    hard_bounces: parseInt(campaign.hardbounces),
    soft_bounces: parseInt(campaign.softbounces),
    unsubscribes: parseInt(campaign.unsubscribes),
  }
}

function formatDate (date) {
    return date.getFullYear() + '-' + ('0'+(date.getMonth()+1)).slice(-2) + '-' + date.getDate() + ' ' +
      ('0'+date.getHours()).slice(-2) + ':' +
      ('0'+date.getMinutes()).slice(-2) + ':' +
      ('0'+date.getSeconds()).slice(-2);
}

exports.helloWorld = async function (req, res) {  
  try {
    let campaigns = await getCampaigns();
    insertRowsToGBQ(campaigns);

  } catch (error) {
    res.status(500).send('NOT ok');
  }
};
