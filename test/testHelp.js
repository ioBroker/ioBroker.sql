const assert = require('node:assert');
const fs = require('node:fs');
const path = require('node:path');

// Needs build/: like testMessageGuard.js, it reads the module of its own and not build/main.js
const { COMMANDS, HELP } = require('../build/lib/help');
const { guardUninitialized } = require('../build/lib/messages');

/** The commands `#dispatchMessage` in src/main.ts answers */
function commandsOfMain() {
    const source = fs.readFileSync(path.join(__dirname, '..', 'src', 'main.ts'), 'utf8');
    const start = source.indexOf('#dispatchMessage(msg');
    const end = source.indexOf('processStartValues(', start);
    assert.ok(start > 0 && end > start, '#dispatchMessage found');
    return [...source.slice(start, end).matchAll(/msg\.command === '([^']+)'/g)].map(m => m[1]);
}

/** Whether the text names the command as a word of its own */
function names(text, command) {
    return new RegExp(`(^|[^\\w])${command}([^\\w]|$)`).test(text);
}

describe('Test help', function () {
    it('lists every command #dispatchMessage answers, each once', function () {
        const commands = COMMANDS.map(c => c.command);
        assert.strictEqual(new Set(commands).size, commands.length, 'no duplicates');
        assert.deepStrictEqual([...commands].sort(), [...new Set(commandsOfMain())].sort());
    });

    it('describes every command', function () {
        const list = HELP.slice(HELP.indexOf('Commands (sendTo sql.0):'), HELP.indexOf('How to talk to the user:'));
        for (const { command } of COMMANDS.filter(c => c.command !== 'help')) {
            assert.ok(names(list, command), command);
        }
    });

    it('reserves exactly the commands with write: true for the request of the user', function () {
        const rules = HELP.slice(HELP.indexOf('How to talk to the user:'));
        const rule = rules
            .split('\n- ')
            .find(r => r.replace(/\s+/g, ' ').includes("never without the user's explicit request"));
        assert.ok(rule, 'rule for the commands that change something');
        for (const { command, write } of COMMANDS) {
            assert.strictEqual(names(rule, command), write, command);
        }
    });

    it('says that query may only send SELECT without the request of the user', function () {
        assert.ok(HELP.includes('Send only SELECT statements with query'));
    });

    it('answers help also before the database is known', function () {
        assert.strictEqual(guardUninitialized('help', false), null);
    });

    it('keeps the lines short', function () {
        for (const line of HELP.split('\n')) {
            assert.ok(line.length <= 120, line);
        }
    });

    it('marks the standard commands of the shared catalog', function () {
        const marked = COMMANDS.filter(c => c.standard).map(c => [c.command, c.standard]);
        assert.deepStrictEqual(marked, [
            ['getHistory', 'history.getHistory'],
            ['aggregate', 'history.aggregate'],
            ['lastChange', 'history.lastChange'],
        ]);
        for (const { command, write, standard } of COMMANDS.filter(c => c.standard)) {
            assert.strictEqual(write, false, command);
            assert.strictEqual(standard, `history.${command}`);
        }
        assert.ok(HELP.includes('standard commands of the shared catalog'));
    });
});
