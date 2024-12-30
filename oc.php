<?php
// Read the access token from the file
$accessTokenFile = 'accessToken.txt';
if (file_exists($accessTokenFile)) {
    $accessToken = trim(file_get_contents($accessTokenFile));
} else {
    die("Error: Access token file not found.");
}

// Define the URL and parameters for the API request
$url = 'https://api.upstox.com/v2/option/chain';
$params = [
    'instrument_key' => 'NSE_INDEX|Nifty 50',
    'expiry_date' => '2024-03-28'
];

// Set the headers with the access token
$headers = [
    'Accept: application/json',
    "Authorization: Bearer $accessToken"
];

// Initialize the cURL session
$ch = curl_init();
curl_setopt($ch, CURLOPT_URL, $url . '?' . http_build_query($params));
curl_setopt($ch, CURLOPT_HTTPHEADER, $headers);
curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);

// Execute the API request
$response = curl_exec($ch);
curl_close($ch);

// Output the API response
echo $response;
?>
