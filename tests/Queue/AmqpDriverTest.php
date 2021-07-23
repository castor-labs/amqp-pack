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
use Castor\Net\InvalidUri;
use Castor\Net\Uri;
use Generator;
use PHPUnit\Framework\TestCase;

/**
 * Class AmqpDriverTest.
 *
 * @internal
 * @covers \Castor\Queue\AmqpDriver
 */
class AmqpDriverTest extends TestCase
{
    /**
     * @throws InvalidUri
     */
    public function testItPublishes5000Messages(): void
    {
        $factory = new AmqpFactory(new ConnectionManager());
        $driver = $factory->create(Uri::parse('amqp://localhost'));
        $count = 0;
        foreach ($this->randomMessages() as $message) {
            ++$count;
            $driver->publish('messages', $message);
        }
        self::assertSame(5000, $count);
    }

    /**
     * @depends testItPublishes5000Messages
     *
     * @throws InvalidUri
     */
    public function testItConsumes5000Messages(): void
    {
        $factory = new AmqpFactory(new ConnectionManager());
        $driver = $factory->create(Uri::parse('amqp://localhost'));
        $cancelWrapper = static function (callable $cancel): callable {
            return static function (int $count) use ($cancel) {
                self::assertSame(5000, $count);
                $cancel();
            };
        };
        $count = 0;
        $driver->consume('messages', function (string $message, callable $cancel) use ($cancelWrapper, &$count) {
            $cancel = $cancelWrapper($cancel);
            self::assertSame('This is a dummy message', $message);
            ++$count;
            if (5000 === $count) {
                $cancel($count);
            }
        });
    }

    protected function randomMessages(): Generator
    {
        for ($i = 0; $i < 5000; ++$i) {
            yield 'This is a dummy message';
        }
    }
}
