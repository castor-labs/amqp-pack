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

namespace Castor\Amqp;

use Castor\Net\Uri;
use Exception;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Connection\AMQPLazyConnection;
use PhpAmqpLib\Connection\AMQPLazySocketConnection;
use PhpAmqpLib\Connection\AMQPLazySSLConnection;
use PhpAmqpLib\Connection\AMQPSocketConnection;
use PhpAmqpLib\Connection\AMQPSSLConnection;
use PhpAmqpLib\Connection\AMQPStreamConnection;

/**
 * Class ConnectionManager.
 *
 * @psalm-suppress MixedAssignment
 * @psalm-suppress InvalidScalarArgument
 * @psalm-suppress MixedArgument
 */
class ConnectionManager
{
    private const SCHEME_AMQP = 'amqp';
    private const SCHEME_SOCKET = 'amqp+socket';
    private const SCHEME_STREAM = 'amqp+stream';
    private const SCHEME_SSL = 'amqps';

    private const RABBIT_PORT = '5672';

    private static array $schemes = [self::SCHEME_AMQP, self::SCHEME_SOCKET, self::SCHEME_STREAM, self::SCHEME_SSL];

    /**
     * @var array<string,AbstractConnection>
     */
    private array $connections;

    /**
     * ConnectionManager constructor.
     */
    public function __construct()
    {
        $this->connections = [];
    }

    /**
     * @return array|string[]
     */
    public static function getSchemes(): array
    {
        return self::$schemes;
    }

    /**
     * @throws Exception
     */
    public function getConnection(Uri $uri): AbstractConnection
    {
        $key = $uri->toStr();
        $connection = $this->connections[$key] ?? null;
        if ($connection instanceof AbstractConnection) {
            return $connection;
        }
        $connection = $this->createConnection($uri);
        $this->connections[$key] = $connection;

        return $connection;
    }

    public function clearConnections(): void
    {
        $this->connections = [];
    }

    /**
     * @return AbstractConnection[]
     */
    public function getConnections(): array
    {
        return $this->connections;
    }

    /**
     * @throws Exception
     */
    private function createConnection(Uri $uri): AbstractConnection
    {
        $scheme = $uri->getScheme();
        $query = Uri\Query::parse($uri->getQuery());

        $host = $uri->getHost() ?: 'localhost';
        $port = $uri->getPort() ?: self::RABBIT_PORT;
        $user = $uri->getUser() ?: 'guest';
        $pass = $uri->getPass() ?: 'guest';
        $vhost = $uri->getPath() ?: '/';

        $lazy = (bool) ($query->get('lazy')[0] ?? false);
        $insist = (bool) ($query->get('insist')[0] ?? false);
        $loginMethod = $query->get('loginMethod')[0] ?? 'AMQPLAIN';
        $locale = $query->get('locale')[0] ?? 'en_US';
        $connTimeout = (float) ($query->get('connectionTimeout')[0] ?? '3.0');
        $readWriteTimeout = (float) ($query->get('readWriteTimeout')[0] ?? '3.0');
        $readTimeout = (int) ($query->get('readTimeout')[0] ?? '3');
        $writeTimeout = (int) ($query->get('readTimeout')[0] ?? '3');

        $caPath = $query->get('caPath')[0] ?? '/etc/ssl/certs';
        $caFile = $query->get('caFile')[0] ?? '';
        $verifyPeer = (bool) ($query->get('verifyPeer')[0] ?? true);

        if ('amqp+socket' === $scheme || 'amqp' === $scheme) {
            $params = [$host, $port, $user, $pass, $vhost, $insist, $loginMethod, null, $locale, $readTimeout, false, $writeTimeout];
            if (true === $lazy) {
                return new AMQPLazySocketConnection(...$params);
            }

            return new AMQPSocketConnection(...$params);
        }

        if ('amqps' === $scheme) {
            $sslOptions = [
                'capath' => $caPath,
                'cafile' => $caFile,
                'verify_peer' => $verifyPeer,
            ];

            $options = [
                'insist' => $insist,
                'login_method' => $loginMethod,
                'locale' => $locale,
                'connection_timeout' => $connTimeout,
                'read_write_timeout' => $readWriteTimeout,
            ];

            $params = [$host, $port, $user, $pass, $vhost, $sslOptions, $options];

            if (true === $lazy) {
                return new AMQPLazySSLConnection(...$params);
            }

            return new AMQPSSLConnection(...$params);
        }

        $params = [$host, $port, $user, $pass, $vhost, $insist, $loginMethod, null, $locale, $connTimeout, $readWriteTimeout];

        if (true === $lazy) {
            return new AMQPLazyConnection(...$params);
        }

        return new AMQPStreamConnection(...$params);
    }
}
