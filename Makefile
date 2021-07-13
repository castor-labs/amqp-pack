stack:
	docker-compose up -d

test: stack
	XDEBUG_MODE=coverage vendor/bin/phpunit --testdox --coverage-text

lint:
	vendor/bin/php-cs-fixer fix

type-check:
	vendor/bin/psalm --stats --no-cache --show-info=true