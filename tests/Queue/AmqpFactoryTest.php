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

use Castor\Net\InvalidUri;
use Castor\Net\Uri;
use PhpAmqpLib\Connection\AMQPLazyConnection;
use PhpAmqpLib\Connection\AMQPLazySocketConnection;
use PhpAmqpLib\Connection\AMQPSocketConnection;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PHPUnit\Framework\TestCase;

/**
 * Class AmqpFactoryTest.
 *
 * @covers \Castor\Queue\AmqpFactory
 *
 * @internal
 */
final class AmqpFactoryTest extends TestCase
{
    /**
     * @throws InvalidUri
     */
    public function testItCreatesLazySocketConnection(): void
    {
        $factory = new AmqpFactory();
        $driver = $factory->create(Uri::parse('amqp://localhost?lazy=true'));
        self::assertInstanceOf(AMQPLazySocketConnection::class, $driver->getConnection());
    }

    /**
     * @throws InvalidUri
     */
    public function testItCreatesSocketConnection(): void
    {
        $factory = new AmqpFactory();
        $driver = $factory->create(Uri::parse('amqp://localhost'));
        self::assertInstanceOf(AMQPSocketConnection::class, $driver->getConnection());
    }

    /**
     * @throws InvalidUri
     */
    public function testItCreatesLazyStreamConnection(): void
    {
        $factory = new AmqpFactory();
        $driver = $factory->create(Uri::parse('amqp+stream://localhost?lazy=true'));
        self::assertInstanceOf(AMQPLazyConnection::class, $driver->getConnection());
    }

    /**
     * @throws InvalidUri
     */
    public function testItCreatesStreamConnection(): void
    {
        $factory = new AmqpFactory();
        $driver = $factory->create(Uri::parse('amqp+stream://localhost'));
        self::assertInstanceOf(AMQPStreamConnection::class, $driver->getConnection());
    }
}
