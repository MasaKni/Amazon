<?php
/**
 * This file is part of rshop/rshop package.
 *
 * (c) RIESENIA.com
 */

declare(strict_types=1);

namespace Rshop\Amazon;

use Rshop\Core\BasePlugin;

class Plugin extends BasePlugin
{
    /**
     * {@inheritdoc}
     */
    public function getCacheEngines(): array
    {
        return [];
    }

    /**
     * {@inheritdoc}
     */
    public function getTables(): array
    {
        return [];
    }
}
