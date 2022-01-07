<?php
/**
 * This file is part of rshop/rshop package.
 *
 * (c) RIESENIA.com
 */

declare(strict_types=1);

namespace Rshop\Amazon\Module\Synchronization;

use Cake\ORM\Query;
use Cake\Utility\Xml;
use Rshop\Synchronization\Module\Synchronization\AbstractSynchronization;
use Rshop\Synchronization\Module\Synchronization\CsvReaderTrait;
use Rshop\Synchronization\Module\Synchronization\IteratorNextTrait;
use SellingPartnerApi\Api\FeedsApi;
use SellingPartnerApi\Api\OrdersApi;
use SellingPartnerApi\Api\ReportsApi;
use SellingPartnerApi\ApiException;
use SellingPartnerApi\Configuration;
use SellingPartnerApi\Endpoint;
use SellingPartnerApi\Model\Feeds\CreateFeedDocumentSpecification;
use SellingPartnerApi\Model\Feeds\CreateFeedSpecification;
use SellingPartnerApi\Model\Orders\Address as OrderAddress;
use SellingPartnerApi\Model\Orders\BuyerInfo;
use SellingPartnerApi\Model\Orders\Order;
use SellingPartnerApi\Model\Reports\CreateReportSpecification;

class Amazon extends AbstractSynchronization
{
    use CsvReaderTrait, IteratorNextTrait {
        IteratorNextTrait::_next insteadof CsvReaderTrait;
        CsvReaderTrait::_next as _nextCsv;
    }

    const MARKETPLACES = [
        // North America
        'A2Q3Y263D00KWC' => 'Brazil',
        'A2EUQ1WTGCTBG2' => 'Canada',
        'A1AM78C64UM0Y8' => 'Mexico',
        'ATVPDKIKX0DER' => 'US',
        // Europe
        'A2VIGQ35RCS4UG' => 'U.A.E.',
        'A1PA6795UKMFR9' => 'Germany',
        'ARBP9OOSHTCHU' => 'Egypt',
        'A1RKKUPIHCS9HS' => 'Spain',
        'A13V1IB3VIYZZH' => 'France',
        'A1F83G8C2ARO7P' => 'UK',
        'A21TJRUUN4KGV' => 'India',
        'APJ6JRA9NG5V4' => 'Italy',
        'A1805IZSGTT6HS' => 'Netherlands',
        'A1C3SOZRARQ6R3' => 'Poland',
        'A17E79C6D8DWNP' => 'Saudi Arabia',
        'A2NODRKZP88ZB9' => 'Sweden',
        'A33AVAJ2PDY3EV' => 'Turkey',
        // Far East
        'A19VAU5U5O7RUS' => 'Singapore',
        'A39IBJ37TRP1C6' => 'Australia',
        'A1VC38T7YXB528' => 'Japan'
    ];

    /** @var array<string,string> */
    protected $_feedTypes = [
        'ProductAvailabilities' => 'POST_INVENTORY_AVAILABILITY_DATA'
    ];

    /** @var array<string,string> */
    protected $_messageTypes = [
        'ProductAvailabilities' => 'Inventory'
    ];

    /** @var Configuration */
    protected $_configuration;

    /** @var string */
    protected $_sellerId;

    /** @var string */
    protected $_mainMarketplace;

    /** @var string[] */
    protected $_marketplaces;

    /** @var mixed[] */
    protected $_processedData;

    /**
     * {@inheritdoc}
     */
    public function getName(): string
    {
        return 'Amazon';
    }

    /**
     * {@inheritdoc}
     */
    public function getConfig(): array
    {
        return [
            'sellerId' => [
                'type' => 'text',
                'label' => 'Merchant Identifier'
            ],
            'awsAccessKeyId' => [
                'type' => 'text',
                'label' => 'AWS Access Key ID'
            ],
            'awsSecretAccessKey' => [
                'type' => 'password',
                'label' => 'AWS Secret Access Key'
            ],
            'roleArn' => [
                'type' => 'text',
                'label' => 'AWS Role ARN'
            ],
            'lwaClientId' => [
                'type' => 'text',
                'label' => 'LWA Client ID'
            ],
            'lwaClientSecret' => [
                'type' => 'password',
                'label' => 'LWA Client Secret'
            ],
            'lwaRefreshToken' => [
                'type' => 'text',
                'label' => 'LWA Refresh Token'
            ],
            'mainMarketplace' => [
                'type' => 'select',
                'label' => __d('rshop', 'Hlavný Marketplace'),
                'help' => __d('rshop', 'Používa sa pre párovanie zalistovaných produktov na základe SKU.'),
                'options' => static::MARKETPLACES
            ],
            'marketplaces[]' => [
                'type' => 'select',
                'label' => __d('rshop', 'Aktívne Marketplaces'),
                'multiple' => true,
                'help' => __d('rshop', 'Môžu byť zadané iba marketplaces spadajúce pod rovnaký endpoint.'),
                'options' => static::MARKETPLACES
            ]
        ];
    }

    /**
     * {@inheritdoc}
     */
    public function import(array $config, int $typeId, string $name)
    {
        $this->_connect($config);
        $this->_setData($this->_getServiceDataIterator($name));

        parent::import($config, $typeId, $name);
    }

    /**
     * {@inheritdoc}
     */
    public function export(array $config, int $typeId, string $name)
    {
        $this->_connect($config);

        parent::export($config, $typeId, $name);

        if (!$this->_exportData) {
            return;
        }

        /** @var \SimpleXMLElement $xmlData */
        $xmlData = Xml::fromArray(['AmazonEnvelope' => [
            'Header' => [
                'DocumentVersion' => '1.01',
                'MerchantIdentifier' => $this->_sellerId
            ],
            'MessageType' => $this->_messageTypes[$name],
            'Message' => $this->_exportData
        ]]);

        $xmlData->addAttribute('xsi:noNamespaceSchemaLocation', 'amzn-envelope.xsd', 'http://www.w3.org/2001/XMLSchema-instance');

        $xml = (string) $xmlData->asXML();

        $api = new FeedsApi($this->_configuration);

        // create a feed document
        $result = $api->createFeedDocument(new CreateFeedDocumentSpecification(['content_type' => 'text/xml; charset=UTF-8']));

        // upload the feed data
        $curl = \curl_init();

        \curl_setopt_array($curl, [
            CURLOPT_URL => $result->getUrl(),
            CURLOPT_SSL_VERIFYHOST => 0,
            CURLOPT_SSL_VERIFYPEER => 0,
            CURLOPT_ENCODING => '',
            CURLOPT_MAXREDIRS => 10,
            CURLOPT_TIMEOUT => 90,
            CURLOPT_HTTP_VERSION => CURL_HTTP_VERSION_1_1,
            CURLOPT_FOLLOWLOCATION => true,
            CURLOPT_CUSTOMREQUEST => 'PUT',
            CURLOPT_POSTFIELDS => $xml,
            CURLOPT_HTTPHEADER => [
                'Accept: application/xml',
                'Content-Type: text/xml; charset=UTF-8'
            ],
        ]);

        \curl_exec($curl);

        if ($error = \curl_error($curl)) {
            throw new ApiException('Feed document upload failed: ' . $error);
        }

        \curl_close($curl);

        // create a feed
        $specification = new CreateFeedSpecification([
            'feed_type' => $this->_feedTypes[$name],
            'marketplace_ids' => $this->_marketplaces,
            'input_feed_document_id' => $result->getFeedDocumentId()
        ]);

        $feedId = $api->createFeed($specification)->getFeedId();

        do {
            \sleep(5);
            $feed = $api->getFeed($feedId);

            if ($feed->getProcessingStatus() == $feed::PROCESSING_STATUS_FATAL || $feed->getProcessingStatus() == $feed::PROCESSING_STATUS_CANCELLED) {
                throw new ApiException('Feed not processed with status ' . $feed->getProcessingStatus());
            }
        } while ($feed->getProcessingStatus() == $feed::PROCESSING_STATUS_IN_QUEUE || $feed->getProcessingStatus() == $feed::PROCESSING_STATUS_IN_PROGRESS);

        if (!$feed->getResultFeedDocumentId()) {
            throw new ApiException('Feed does not contain document ID');
        }

        $document = $api->getfeedDocument($feed->getResultFeedDocumentId());
        \file_put_contents(TMP . 'amazon-feed-report.xml', \file_get_contents($document->getUrl()));

        // process response (gzipped)
        if (\method_exists($this, 'process' . $name . 'Response')) {
            $responseXml = \simplexml_load_file(TMP . 'amazon-feed-report.xml');

            $this->{'process' . $name . 'Response'}($responseXml);
        }
    }

    /**
     * Import orders.
     *
     * @return array|null
     */
    public function importOrders(): ?array
    {
        $data = $this->_next();

        if (!$data) {
            return null;
        }

        /** @var Order $order */
        $order = $data[0];

        /** @var \SellingPartnerApi\Model\Orders\OrderItem[] $items */
        $items = $data[1];

        $entity = $this->_buildOrderEntity($order, $items);

        if (!$entity) {
            return [null, null];
        }

        return [$order->getAmazonOrderId(), $entity];
    }

    /**
     * Import products.
     *
     * @return array|null
     */
    public function importProducts(): ?array
    {
        $data = $this->_next();

        if (!$data) {
            return null;
        }

        $sku = $data['seller-sku'];

        if (!$sku) {
            return [null, null];
        }

        // pair by EAN
        if (!$this->_getForeignKey('Products', $sku)) {
            $p = $this->synchronizationsTable()->getSynchronizedTable('Products')
                ->findByEan($sku)
                ->select(['id', 'model'])
                ->first();

            if ($p) {
                $this->_saveRemoteKey('Products', $p['id'], $sku);
                $this->synchronizationsTable()->addImportSuccess('Products', $sku);
            } else {
                $this->synchronizationsTable()->addImportError('Products', $sku, 'Product with SKU ' . $sku . ' not found.');

                return [null, null];
            }
        }

        $this->_updateProductByAmazonData($data);

        return [null, null];
    }

    /**
     * Export product availabilities.
     *
     * @param array|\ArrayAccess $entity
     *
     * @return int|null
     */
    public function exportProductAvailabilities($entity): ?int
    {
        if (!$this->_getRemoteKey('Products', $entity['id'])) {
            return null;
        }

        $stock = $this->_getAmazonStock($entity);

        if (!$this->_checkProductData($entity, 'amazon_availability', (string) $stock)) {
            $this->_exportData[] = [
                'MessageID' => $entity['id'],
                'OperationType' => 'Update',
                'Inventory' => [
                    'SKU' => $this->_getRemoteKey('Products', $entity['id']),
                    'Quantity' => $stock
                ]
            ];

            $this->_processedData[$entity['id']] = $stock;
        }

        return null;
    }

    /**
     * Process response from product availabilities import.
     *
     * @param \SimpleXMLElement $xml
     */
    public function processProductAvailabilitiesResponse(\SimpleXMLElement $xml)
    {
        foreach ($this->_processedData as $id => $stock) {
            $this->synchronizationsTable()->getSynchronizedTable('Products')->updateAll(['amazon_availability' => $stock], ['id' => $id]);
            $this->synchronizationsTable()->addImportSuccess('Products', $this->_getRemoteKey('Products', $id));
        }
    }

    /**
     * Build order entity.
     *
     * @param Order                                       $order
     * @param \SellingPartnerApi\Model\Orders\OrderItem[] $items
     *
     * @return array|null
     */
    protected function _buildOrderEntity(Order $order, array $items): ?array
    {
        // already exists
        if ($this->_getForeignKey('Orders', $order->getAmazonOrderId())) {
            return null;
        }

        $buyerInfo = $order->getBuyerInfo() ?? new BuyerInfo();
        $shippingAddress = $order->getShippingAddress() ?? new OrderAddress();

        // currency
        $firstPrice = $items[0]->getItemPrice();
        $currencyCode = $firstPrice ? $firstPrice->getCurrencyCode() : 'EUR';
        $currencyId = $this->synchronizationsTable()->getSynchronizedTable('Currencies')
            ->findByCode($currencyCode)
            ->select(['id'])
            ->first()['id'] ?? null;

        $entity = [
            'amazon_id' => $order->getAmazonOrderId(),
            'amazon_status' => $order->getOrderStatus(),
            'customer_email' => $buyerInfo->getBuyerEmail(),
            'customer_phone' => $shippingAddress->getPhone(),
            'billing_name' => $shippingAddress->getName(),
            'billing_address' => $shippingAddress->getAddressLine1(),
            'billing_address2' => $shippingAddress->getAddressLine2(),
            'billing_city' => $shippingAddress->getCity(),
            'billing_post_code' => $shippingAddress->getPostalCode(),
            'billing_state_name' => $shippingAddress->getStateOrRegion(),
            'billing_country_id' => $this->_countryIdByField('iso2code', $shippingAddress->getCountryCode()),
            'shipping_name' => $shippingAddress->getName(),
            'shipping_address' => $shippingAddress->getAddressLine1(),
            'shipping_address2' => $shippingAddress->getAddressLine2(),
            'shipping_city' => $shippingAddress->getCity(),
            'shipping_post_code' => $shippingAddress->getPostalCode(),
            'shipping_state_name' => $shippingAddress->getStateOrRegion(),
            'shipping_country_id' => $this->_countryIdByField('iso2code', $shippingAddress->getCountryCode()),
            'locale' => 'sk_SK',
            'language_id' => 1,
            'currency_code' => $currencyCode,
            'currency_id' => $currencyId,
            'no_email' => true,
            'duplicate' => true,
            'order_products' => []
        ];

        $shippingPrice = 0;
        $shippingTax = 0;
        $shippingDiscount = 0;
        $shippingDiscountTax = 0;
        $promotionDiscount = 0;
        $promotionDiscountTax = 0;

        foreach ($items as $item) {
            $unitPrice = \round($this->_moneyAmount($item->getItemPrice()) / $item->getQuantityOrdered(), 4);
            $taxValue = \round($this->_moneyAmount($item->getItemTax()) / $item->getQuantityOrdered(), 4);
            $taxRate = \round(100 * (1 - ($unitPrice - $taxValue) / $unitPrice));

            $data = [
                'type' => 'product',
                'name' => $item->getTitle(),
                'quantity' => $item->getQuantityOrdered(),
                'unit_price' => $unitPrice,
                'tax_rate' => $taxRate
            ];

            if ($this->_getForeignKey('Products', $item->getSellerSku())) {
                $p = $this->synchronizationsTable()->getSynchronizedTable('Products')
                    ->findById($this->_getForeignKey('Products', $item->getSellerSku()))
                    ->select(['id', 'model'])
                    ->first();

                if ($p) {
                    $data['product_id'] = $p['id'];
                    $data['model'] = $p['model'];
                }
            }

            // napocitame hodnoty
            $shippingPrice += $this->_moneyAmount($item->getShippingPrice());
            $shippingTax += $this->_moneyAmount($item->getShippingTax());
            $shippingDiscount += $this->_moneyAmount($item->getShippingDiscount());
            $shippingDiscountTax += $this->_moneyAmount($item->getShippingDiscountTax());
            $promotionDiscount += $this->_moneyAmount($item->getPromotionDiscount());
            $promotionDiscountTax += $this->_moneyAmount($item->getPromotionDiscountTax());

            $entity['order_products'][] = $data;
        }

        // shipping
        $shippingPrice -= $shippingDiscount;
        $shippingTax -= $shippingDiscountTax;

        if ($shippingPrice > 0) {
            $entity['order_products'][] = [
                'type' => 'shipping',
                'name' => 'Doprava',
                'quantity' => 1,
                'unit_price' => $shippingPrice,
                'tax_rate' => \round(100 * (1 - ($shippingPrice - $shippingTax) / $shippingPrice))
            ];
        }

        // promotion
        if ($promotionDiscount > 0) {
            $entity['order_products'][] = [
                'type' => 'discount',
                'name' => 'Zľava',
                'quantity' => 1,
                'unit_price' => $promotionDiscount,
                'tax_rate' => \round(100 * (1 - ($promotionDiscount - $promotionDiscountTax) / $promotionDiscount))
            ];
        }

        return $entity;
    }

    /**
     * Update product data.
     *
     * @param array $data
     */
    protected function _updateProductByAmazonData(array $data)
    {
        $this->synchronizationsTable()->getSynchronizedTable('Products')->updateAll(['amazon_availability' => (int) $data['quantity']], ['id' => $this->_getForeignKey('Products', $data['seller-sku'])]);
    }

    /**
     * Setup connection.
     *
     * @param array $config
     */
    protected function _connect(array $config)
    {
        $this->_sellerId = $config['sellerId'];
        $this->_mainMarketplace = $config['mainMarketplace'];
        $this->_marketplaces = \array_filter($config['marketplaces']);

        // endpoint
        $config['endpoint'] = Endpoint::getByMarketplaceId(\current($this->_marketplaces));

        $this->_configuration = new Configuration(\array_intersect_key($config, \array_flip(['awsAccessKeyId', 'awsSecretAccessKey', 'roleArn', 'lwaClientId', 'lwaClientSecret', 'lwaRefreshToken', 'endpoint'])));
    }

    /**
     * Get service data getter by table name.
     *
     * @param string $name
     *
     * @return \Iterator
     */
    protected function _getServiceDataIterator(string $name): \Iterator
    {
        switch ($name) {
            case 'Orders':
                return $this->_getOrders();

            case 'Products':
                return $this->_getProducts();
        }

        throw new \OutOfBoundsException('Undefined service data getter for ' . $name);
    }

    /**
     * Get orders.
     *
     * @return \Generator
     */
    protected function _getOrders(): \Generator
    {
        $api = new OrdersApi($this->_configuration);
        $last = ($this->_getLastModified($this->_typeId, 'Orders') ?? new \DateTimeImmutable('1 month ago'))->setTimezone(new \DateTimeZone('Europe/London'))->format('Y-m-d\TH:i:s\Z');
        $now = new \DateTimeImmutable('-1 minute');

        do {
            $burst = 0;
            $result = $api->getOrders($this->_marketplaces, null, null, $last, null, [Order::ORDER_STATUS_UNSHIPPED], null, null, null, null, null, null, $nextToken ?? null);

            if (!$result->valid() || !($orders = $result->getPayload())) {
                throw new ApiException('Cannot get Amazon orders: ' . \json_encode($result->getErrors()), 0, $result->getHeaders());
            }

            foreach ($orders->getOrders() as $order) {
                // if already exists, do not fetch items
                if ($this->_getForeignKey('Orders', $order->getAmazonOrderId())) {
                    continue;
                }

                // fetch order items
                $items = [];

                do {
                    if (++$burst == 20) {
                        $burst = 0;
                        \sleep(3600);
                    }

                    $result = $api->getOrderItems($order->getAmazonOrderId(), $nextItemsToken ?? null);

                    if (!$result->valid() || !($orderItems = $result->getPayload())) {
                        throw new ApiException('Cannot get Amazon order items for order ' . $order->getAmazonOrderId() . ': ' . \json_encode($result->getErrors()), 0, $result->getHeaders());
                    }

                    $items = \array_merge($items, $orderItems->getOrderItems());
                } while ($nextItemsToken = $orderItems->getNextToken());

                yield [$order, $items];
            }
        } while ($nextToken = $orders->getNextToken());

        $this->_updateLastModified($this->_typeId, 'Orders', $now);
    }

    /**
     * Get products.
     *
     * @return \Generator
     */
    protected function _getProducts(): \Generator
    {
        $api = new ReportsApi($this->_configuration);

        $specification = new CreateReportSpecification([
            'report_type' => 'GET_MERCHANT_LISTINGS_ALL_DATA',
            'marketplace_ids' => [$this->_mainMarketplace]
        ]);

        $reportId = $api->createReport($specification)->getReportId();

        do {
            \sleep(5);
            $report = $api->getReport($reportId);

            if ($report->getProcessingStatus() == $report::PROCESSING_STATUS_FATAL || $report->getProcessingStatus() == $report::PROCESSING_STATUS_CANCELLED) {
                throw new ApiException('Report not created with status ' . $report->getProcessingStatus());
            }
        } while ($report->getProcessingStatus() == $report::PROCESSING_STATUS_IN_QUEUE || $report->getProcessingStatus() == $report::PROCESSING_STATUS_IN_PROGRESS);

        if (!$report->getReportDocumentId()) {
            throw new ApiException('Report does not contain document ID');
        }

        $document = $api->getReportDocument($report->getReportDocumentId(), 'GET_MERCHANT_LISTINGS_ALL_DATA');
        \file_put_contents(TMP . 'amazon-report.csv', \file_get_contents($document->getUrl()));

        $this->_delimeter = "\t";
        $this->_openCsvFile(TMP . 'amazon-report.csv');

        while ($row = $this->_nextCsv()) {
            yield $row;
        }
    }

    /**
     * Get product stock.
     *
     * @param array|\ArrayAccess $entity
     *
     * @return int
     */
    protected function _getAmazonStock($entity): int
    {
        return (int) $entity['stock'] > 0 ? (int) $entity['stock'] : 0;
    }

    /**
     * Check if product data have changed.
     *
     * @param array|\ArrayAccess $entity
     * @param string             $field
     * @param string             $value
     *
     * @return bool
     */
    protected function _checkProductData($entity, string $field, string $value): bool
    {
        return $entity[$field] === $value;
    }

    /**
     * Get money amount.
     *
     * @param \SellingPartnerApi\Model\Orders\Money|null $money
     *
     * @return float
     */
    protected function _moneyAmount($money): float
    {
        if (!$money) {
            return 0.0;
        }

        return (float) $money->getAmount();
    }

    /**
     * {@inheritdoc}
     */
    protected function _exportData(int $typeId, string $tableName): Query
    {
        // ProductAvailabilities are just Products
        if (\in_array($tableName, ['ProductAvailabilities'])) {
            /** @var Query $q */
            $q = $this->synchronizationsTable()->exportProducts($typeId, false)->join($this->synchronizationsTable()->joinDefinition($typeId, 'Products', 'INNER'));

            return $q;
        }

        return parent::_exportData($typeId, $tableName);
    }
}
