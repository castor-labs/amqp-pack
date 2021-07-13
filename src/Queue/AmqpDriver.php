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

use ErrorException;
use Exception;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Message\AMQPMessage;

/**
 * Class AmqpDriver.
 */
final class AmqpDriver implements Driver
{
    private AbstractConnection $connection;
    private ?AMQPChannel $channel;

    /**
     * AmqpDriver constructor.
     */
    public function __construct(AbstractConnection $connection)
    {
        $this->connection = $connection;
        $this->channel = null;
    }

    public function __destruct()
    {
        if (null !== $this->channel) {
            $this->channel->close();
        }
    }

    public function publish(string $queue, string $message): void
    {
        $channel = $this->getChannel();
        $channel->queue_declare($queue, false, true, false, false);
        $amqpMessage = new AMQPMessage($message, [
            'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT,
        ]);
        $channel->basic_publish($amqpMessage, '', $queue);
    }

    public function purge(string $queue): void
    {
        $channel = $this->getChannel();
        $channel->queue_purge($queue);
    }

    public function consume(string $queue, callable $callback): void
    {
        $channel = $this->getChannel();
        $channel->queue_declare($queue, false, true, false, false);
        $cancel = function () use ($channel): void {
            $channel->close();
            $this->channel = null;
        };
        $innerCallback = static function (AMQPMessage $message) use ($callback, $cancel): void {
            $callback($message->body, $cancel);
        };

        try {
            $channel->basic_consume($queue, '', false, true, false, false, $innerCallback);
        } catch (Exception $e) {
            throw new DriverError('Could not consume message', 0, $e);
        }

        while ($channel->is_consuming()) {
            try {
                $channel->wait(null, true);
            } catch (ErrorException $e) {
                throw new DriverError('Could not consume message', 0, $e);
            }
        }
    }

    public function getChannel(int $id = null): AMQPChannel
    {
        if (null === $this->channel) {
            $this->channel = $this->connection->channel($id);
        }

        return $this->channel;
    }

    public function getConnection(): AbstractConnection
    {
        return $this->connection;
    }
}
