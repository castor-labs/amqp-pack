<?php

declare(strict_types=1);

/**
 * @project Castor Amqp Pack
 * @link https://github.com/castor-labs/amqp-pack
 * @package castor/amqp-pack
 * @author Matias Navarro-Carter mnavarrocarter@gmail.com
 * @license MIT
 * @copyright 2021 CastorLabs Ltd
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Castor\Queue;

use Castor\Amqp\ConnectionManager;
use Castor\Arr;
use Castor\Net\Uri;
use Exception;

/**
 * Class AmqpFactory.
 */
final class AmqpFactory implements Factory
{
    private ConnectionManager $manager;

    /**
     * AmqpFactory constructor.
     */
    public function __construct(ConnectionManager $manager)
    {
        $this->manager = $manager;
    }

    public function create(Uri $uri): AmqpDriver
    {
        $scheme = $uri->getScheme();
        if (!Arr\has(ConnectionManager::getSchemes(), $scheme)) {
            throw UnsupportedScheme::create($scheme, __CLASS__, ...ConnectionManager::getSchemes());
        }

        try {
            $connection = $this->manager->getConnection($uri);
        } catch (Exception $e) {
            throw new CreationError('Could not create driver', 0, $e);
        }

        return new AmqpDriver($connection);
    }
}
