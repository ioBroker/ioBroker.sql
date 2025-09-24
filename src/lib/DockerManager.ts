// This class implements docker commands using CLI and
// it monitors periodically the docker daemon status.
// It manages containers defined in adapter.config.containers and monitors other containers

import { promisify } from 'node:util';
import { exec } from 'node:child_process';
import type {
    ContainerConfig,
    ContainerInfo,
    ContainerStats,
    ContainerStatus,
    DiskUsage,
    DockerContainerInspect,
    DockerImageInspect,
    ImageInfo,
    ContainerName,
    ImageName,
    NetworkInfo,
    NetworkDriver,
    VolumeInfo,
    VolumeDriver,
} from './dockerManager.types';
import { createConnection } from 'node:net';
import Docker, { type MountPropagation, type MountSettings, type MountType } from 'dockerode';
import type { PackOptions, Pack } from 'tar-fs';

const execPromise = promisify(exec);

const dockerDefaults: Record<string, any> = {
    tty: false,
    stdinOpen: false,
    attachStdin: false,
    attachStdout: false,
    attachStderr: false,
    openStdin: false,
    publishAllPorts: false,
    readOnly: false,
    user: '',
    workdir: '',
    domainname: '',
    macAddress: '',
    networkMode: 'bridge',
};

function isDefault(value: any, def: any): boolean {
    return JSON.stringify(value) === JSON.stringify(def);
}

function deepCompare(object1: any, object2: any): boolean {
    if (typeof object1 === 'number') {
        object1 = object1.toString();
    }
    if (typeof object2 === 'number') {
        object2 = object2.toString();
    }
    if (typeof object1 !== typeof object2) {
        return false;
    }
    if (typeof object1 !== 'object' || object1 === null || object2 === null) {
        return object1 === object2;
    }
    if (Array.isArray(object1)) {
        if (!Array.isArray(object2) || object1.length !== object2.length) {
            return false;
        }
        for (let i = 0; i < object1.length; i++) {
            if (!deepCompare(object1[i], object2[i])) {
                return false;
            }
        }
        return true;
    }
    const keys1 = Object.keys(object1);
    for (const key of keys1) {
        // ignore iob* properties as they belong to ioBroker configuration
        // ignore hostname
        if (key.startsWith('iob') || key === 'hostname') {
            continue;
        }
        if (!deepCompare(object1[key], object2[key])) {
            return false;
        }
    }
    return true;
}

function compareConfigs(desired: ContainerConfig, existing: ContainerConfig): string[] {
    const diffs: string[] = [];

    const keys: (keyof ContainerConfig)[] = Object.keys(desired) as Array<keyof ContainerConfig>;

    // We only compare keys that are in the desired config
    for (const key of keys) {
        // ignore iob* properties as they belong to ioBroker configuration
        // ignore hostname
        if (key.startsWith('iob') || key === 'hostname') {
            continue;
        }
        if (typeof desired[key] === 'object' && desired[key] !== null) {
            if (Array.isArray(desired[key])) {
                if (!Array.isArray(existing[key]) || desired[key].length !== existing[key].length) {
                    diffs.push(key);
                } else {
                    for (let i = 0; i < desired[key].length; i++) {
                        if (!deepCompare(desired[key][i], existing[key][i])) {
                            diffs.push(`${key}[${i}]`);
                        }
                    }
                }
            } else {
                Object.keys(desired[key]).forEach((subKey: string) => {
                    if (!deepCompare((desired as any)[key][subKey], (existing as any)[key][subKey])) {
                        diffs.push(`${key}.${subKey}`);
                    }
                });
            }
        } else if (desired[key] !== existing[key]) {
            diffs.push(key);
        }
    }

    return diffs;
}

// remove undefined entries recursively
function removeUndefined(obj: any): any {
    if (Array.isArray(obj)) {
        const arr = obj.map(v => (v && typeof v === 'object' ? removeUndefined(v) : v)).filter(v => v !== undefined);
        if (!arr.length) {
            return undefined;
        }
        return arr;
    }
    if (obj && typeof obj === 'object') {
        const _obj = Object.fromEntries(
            Object.entries(obj)
                .map(([k, v]) => [k, v && typeof v === 'object' ? removeUndefined(v) : v])
                .filter(
                    ([_, v]) =>
                        v !== undefined &&
                        v !== null &&
                        v !== '' &&
                        !(Array.isArray(v) && v.length === 0) &&
                        !(typeof v === 'object' && Object.keys(v).length === 0),
                ),
        );
        if (Object.keys(_obj).length === 0) {
            return undefined;
        }
        return _obj;
    }
    if (obj === '') {
        return undefined;
    }
    return obj;
}

function cleanContainerConfig(obj: ContainerConfig, mayChange?: boolean): ContainerConfig {
    obj = removeUndefined(obj);

    Object.keys(obj).forEach(name => {
        if (isDefault((obj as any)[name], (dockerDefaults as any)[name])) {
            delete (obj as any)[name];
        }
        if (name === 'mounts') {
            if (!obj.mounts) {
                delete obj.mounts;
                return;
            }
            obj.mounts = obj.mounts.map((mount: any) => {
                const m = { ...mount };
                // /var/lib/docker/volumes/influxdb_0_flux_config/_data
                if (mayChange && m.source.includes('/docker/volumes') && m.source.endsWith('/_data')) {
                    const parts = m.source.split('/');
                    m.source = parts[parts.length - 2];
                }
                delete m.readOnly;
                return m;
            });
            if (!obj.mounts.length) {
                delete obj.mounts;
                return;
            }
            obj.mounts?.sort((a, b) => a.target.localeCompare(b.target));
        }
        if (name === 'ports') {
            if (!obj.ports) {
                delete obj.ports;
                return;
            }
            obj.ports = obj.ports.map((port: any) => {
                const p = { ...port };
                if (p.protocol === 'tcp') {
                    delete p.protocol;
                }
                return p;
            });
            if (!obj.ports.length) {
                delete obj.ports;
                return;
            }
            obj.ports?.sort((a, b) => {
                if (a.hostPort !== b.hostPort) {
                    return parseInt(a.containerPort as string, 10) - parseInt(b.containerPort as string, 10);
                }
                if (a.hostIP !== b.hostIP && a.hostIP && b.hostIP) {
                    return a.hostIP?.localeCompare(b.hostIP);
                }
                return 0;
            });
        }
        if (name === 'environment') {
            if (!obj.environment) {
                delete obj.environment;
                return;
            }
            const env = obj.environment as { [key: string]: string };
            if (Object.keys(env).length) {
                obj.environment = {};
                Object.keys(env)
                    .sort()
                    .forEach(key => {
                        if (key && env[key]) {
                            obj.environment![key] = env[key];
                        }
                    });
            } else {
                delete obj.environment;
            }
            if (!Object.keys(env).length) {
                delete obj.environment;
            }
        }
        if (name === 'labels') {
            if (!obj.labels) {
                delete obj.labels;
                return;
            }
            const labels = obj.labels as { [key: string]: string };
            if (Object.keys(labels).length) {
                obj.labels = {};
                Object.keys(labels)
                    .sort()
                    .forEach(key => {
                        if (key && labels[key]) {
                            obj.labels![key] = labels[key];
                        }
                    });
            } else {
                delete obj.labels;
            }
            if (!Object.keys(labels).length) {
                delete obj.labels;
            }
        }
        if (name === 'volumes') {
            if (!obj.volumes?.length) {
                delete obj.volumes;
                return;
            }
            obj.volumes = obj.volumes.map(v => v.trim()).filter(v => v);
            obj.volumes.sort();
            if (!obj.volumes?.length) {
                delete obj.volumes;
            }
        }
    });

    obj.volumes?.sort();
    return obj;
}

function size2string(size: number): string {
    if (size < 1024) {
        return `${size} B`;
    }
    if (size < 1024 * 1024) {
        return `${(size / 1024).toFixed(2)} KB`;
    }
    if (size < 1024 * 1024 * 1024) {
        return `${(size / (1024 * 1024)).toFixed(2)} MB`;
    }
    return `${(size / (1024 * 1024 * 1024)).toFixed(2)} GB`;
}

export default class DockerManager {
    protected installed: boolean = false;
    protected dockerVersion: string = '';
    protected needSudo: boolean = false;
    readonly #waitReady: Promise<void>;
    readonly #waitAllChecked: Promise<void>;
    #waitAllCheckedResolve: (() => void) | undefined;
    readonly adapter: ioBroker.Adapter;
    readonly #ownContainers: ContainerConfig[] = [];
    #monitoringInterval: NodeJS.Timeout | null = null;
    #ownContainersStats: { [name: string]: ContainerStatus } = {};
    #driver: 'socket' | 'cli' | 'http' | 'https' = 'cli';
    #dockerode: Docker | null = null;
    #cliAvailable: boolean = false;
    protected options: {
        dockerApi?: boolean;
        dockerApiHost?: string;
        dockerApiPort?: number | string;
        dockerApiProtocol?: 'http' | 'https';
    };
    #tarPack: ((cwd: string, opts?: PackOptions) => Pack) | null = null;

    constructor(
        adapter: ioBroker.Adapter,
        options?: {
            dockerApi?: boolean;
            dockerApiHost?: string;
            dockerApiPort?: number | string;
            dockerApiProtocol?: 'http' | 'https';
        },
        containers?: ContainerConfig[],
    ) {
        this.adapter = adapter;
        this.options = options || {};
        this.#ownContainers = containers || [];
        this.#waitReady = new Promise<void>(resolve => this.#init().then(() => resolve()));
        this.#waitAllChecked = new Promise<void>(resolve => (this.#waitAllCheckedResolve = resolve));
    }

    /** Wait till the check if docker is installed and the daemon is running is ready */
    isReady(): Promise<void> {
        return this.#waitReady;
    }

    /**
     * Convert information from inspect to docker configuration to start it
     *
     * @param inspect Inspect information
     */
    static mapInspectToConfig(inspect: DockerContainerInspect): ContainerConfig {
        const obj: ContainerConfig = {
            image: inspect.Config.Image,
            name: inspect.Name.replace(/^\//, ''),
            command: inspect.Config.Cmd ?? undefined,
            entrypoint: inspect.Config.Entrypoint ?? undefined,
            user: inspect.Config.User ?? undefined,
            workdir: inspect.Config.WorkingDir ?? undefined,
            hostname: inspect.Config.Hostname ?? undefined,
            domainname: inspect.Config.Domainname ?? undefined,
            macAddress: inspect.NetworkSettings.MacAddress ?? undefined,
            environment: inspect.Config.Env
                ? Object.fromEntries(
                      inspect.Config.Env.map(e => {
                          const [key, ...rest] = e.split('=');
                          return [key, rest.join('=')];
                      }),
                  )
                : undefined,
            labels: inspect.Config.Labels ?? undefined,
            tty: inspect.Config.Tty,
            stdinOpen: inspect.Config.OpenStdin,
            attachStdin: inspect.Config.AttachStdin,
            attachStdout: inspect.Config.AttachStdout,
            attachStderr: inspect.Config.AttachStderr,
            openStdin: inspect.Config.OpenStdin,
            publishAllPorts: inspect.HostConfig.PublishAllPorts,
            ports: inspect.HostConfig.PortBindings
                ? Object.entries(inspect.HostConfig.PortBindings).flatMap(([containerPort, bindings]) =>
                      bindings.map(binding => ({
                          containerPort: containerPort.split('/')[0],
                          protocol: (containerPort.split('/')[1] as 'tcp' | 'udp') || 'tcp',
                          hostPort: binding.HostPort,
                          hostIP: binding.HostIp,
                      })),
                  )
                : undefined,
            mounts: inspect.Mounts?.map(mount => ({
                type: mount.Type,
                source: mount.Source,
                target: mount.Destination,
                readOnly: mount.RW,
            })),
            volumes: inspect.Config.Volumes ? Object.keys(inspect.Config.Volumes) : undefined,
            extraHosts: inspect.HostConfig.ExtraHosts ?? undefined,
            dns: {
                servers: inspect.HostConfig.Dns,
                search: inspect.HostConfig.DnsSearch,
                options: inspect.HostConfig.DnsOptions,
            },
            networkMode: inspect.HostConfig.NetworkMode,
            networks: inspect.NetworkSettings.Networks
                ? Object.entries(inspect.NetworkSettings.Networks).map(([name, net]) => ({
                      name,
                      aliases: net.Aliases ?? undefined,
                      ipv4Address: net.IPAddress,
                      ipv6Address: net.GlobalIPv6Address,
                      driverOpts: net.DriverOpts ?? undefined,
                  }))
                : undefined,
            restart: {
                policy: inspect.HostConfig.RestartPolicy.Name as any,
                maxRetries: inspect.HostConfig.RestartPolicy.MaximumRetryCount,
            },
            resources: {
                cpuShares: inspect.HostConfig.CpuShares,
                cpuQuota: inspect.HostConfig.CpuQuota,
                cpuPeriod: inspect.HostConfig.CpuPeriod,
                cpusetCpus: inspect.HostConfig.CpusetCpus,
                memory: inspect.HostConfig.Memory,
                memorySwap: inspect.HostConfig.MemorySwap,
                memoryReservation: inspect.HostConfig.MemoryReservation,
                pidsLimit: inspect.HostConfig.PidsLimit ?? undefined,
                shmSize: inspect.HostConfig.ShmSize,
                readOnlyRootFilesystem: inspect.HostConfig.ReadonlyRootfs,
            },
            logging: {
                driver: inspect.HostConfig.LogConfig.Type,
                options: inspect.HostConfig.LogConfig.Config,
            },
            security: {
                privileged: inspect.HostConfig.Privileged,
                capAdd: inspect.HostConfig.CapAdd ?? undefined,
                capDrop: inspect.HostConfig.CapDrop ?? undefined,
                usernsMode: inspect.HostConfig.UsernsMode ?? undefined,
                ipc: inspect.HostConfig.IpcMode,
                pid: inspect.HostConfig.PidMode,
                seccomp:
                    inspect.HostConfig.SecurityOpt?.find(opt => opt.startsWith('seccomp='))?.split('=')[1] ?? undefined,
                apparmor:
                    inspect.HostConfig.SecurityOpt?.find(opt => opt.startsWith('apparmor='))?.split('=')[1] ??
                    undefined,
                groupAdd: inspect.HostConfig.GroupAdd ?? undefined,
                noNewPrivileges: undefined, // Nicht direkt verfügbar
            },
            sysctls: inspect.HostConfig.Sysctls ?? undefined,
            init: inspect.HostConfig.Init ?? undefined,
            stop: {
                signal: inspect.Config.StopSignal ?? undefined,
                gracePeriodSec: inspect.Config.StopTimeout ?? undefined,
            },
            readOnly: inspect.HostConfig.ReadonlyRootfs,
            timezone: undefined, // Nicht direkt verfügbar
            __meta: undefined, // Eigene Metadaten
        };

        return cleanContainerConfig(obj, true);
    }

    /**
     * Get information about the Docker daemon: is it running and which version
     *
     * @returns Object with version and daemonRunning
     */
    async getDockerDaemonInfo(): Promise<{
        version?: string;
        daemonRunning?: boolean;
        removeSupported?: boolean;
        driver: 'socket' | 'cli' | 'http' | 'https';
    }> {
        await this.isReady();
        const daemonRunning = await this.#isDockerDaemonRunning();
        return {
            version: this.dockerVersion,
            daemonRunning,
            removeSupported: !this.#dockerode || this.#cliAvailable,
            driver: this.#driver,
        };
    }

    static checkDockerSocket(): Promise<boolean> {
        return new Promise<boolean>(resolve => {
            const socket = createConnection({ path: '/var/run/docker.sock' }, () => {
                socket.end();
                resolve(true);
            });
            socket.on('error', e => {
                console.error(`Cannot connect to docker socket: ${e.message}`);
                resolve(false);
            });
        });
    }

    static async isDockerApiRunningOnPort(port: number, host = 'localhost'): Promise<boolean> {
        return new Promise(resolve => {
            const socket = createConnection({ port, host }, () => {
                socket.write('GET /version HTTP/1.0\r\nHost: localhost\r\n\r\n');
            });

            let data = '';
            socket.on('data', chunk => (data += chunk.toString()));

            socket.on('end', () => {
                resolve(data.includes('Docker') || data.includes('Api-Version'));
            });

            socket.on('error', () => resolve(false));
        });
    }

    async #init(): Promise<void> {
        // first of all detects which way is available:
        // - '/var/run/docker.sock',
        // - http://localhost:2375,
        // - https://localhost:2376 or
        // - CLI
        // Probe the socket
        if (this.options?.dockerApi && this.options.dockerApiHost && this.options.dockerApiPort) {
            this.#driver = this.options.dockerApiProtocol || 'http';
            this.#dockerode = new Docker({
                host: this.options.dockerApiHost,
                port: this.options.dockerApiPort,
                protocol: this.options.dockerApiProtocol || 'http',
            });
        } else if (await DockerManager.checkDockerSocket()) {
            this.#driver = 'socket';
            this.#dockerode = new Docker({ socketPath: '/var/run/docker.sock' });
        } else if (await DockerManager.isDockerApiRunningOnPort(2375)) {
            this.#driver = 'http';
            this.#dockerode = new Docker({ protocol: 'http', host: '127.0.0.1', port: 2375 });
        } else if (await DockerManager.isDockerApiRunningOnPort(2376)) {
            this.#driver = 'http';
            this.#dockerode = new Docker({ protocol: 'http', host: '127.0.0.1', port: 2376 });
        } else {
            this.#driver = 'cli';
            this.#cliAvailable = true;
        }

        if (!this.#cliAvailable) {
            try {
                const result = await execPromise('docker --version');
                if (!result.stderr && result.stdout) {
                    this.#cliAvailable = true;
                }
            } catch {
                // ignore
            }
        }

        const version = await this.#isDockerInstalled();
        this.installed = !!version;
        if (version) {
            this.dockerVersion = version;
        } else {
            const daemonRunning = await this.#isDockerDaemonRunning();
            if (daemonRunning) {
                // Docker daemon is running, but docker command not found
                this.adapter.log.warn(
                    'Docker daemon is running, but docker command not found. May be "iobroker" user has no access to Docker. Run "iob fix" command to fix it.',
                );
            } else {
                this.adapter.log.warn('Docker is not installed. Please install Docker.');
            }
        }
        if (this.installed) {
            // we still must check the sudo as autocompletion works only via CLI
            this.needSudo = await this.#isNeedSudo();
            await this.#checkOwnContainers();
        } else {
            this.#waitAllCheckedResolve?.();
        }
    }

    async #isDockerDaemonRunning(): Promise<boolean> {
        if (this.#dockerode) {
            return true;
        }
        try {
            const { stdout, stderr } = await execPromise('systemctl status docker');
            // ● docker.service - Docker Application Container Engine
            //      Loaded: loaded (/lib/systemd/system/docker.service; enabled; preset: enabled)
            //      Active: active (running) since Fri 2025-08-15 08:37:22 CEST; 3 weeks 2 days ago
            // TriggeredBy: ● docker.socket
            //        Docs: https://docs.docker.com
            //    Main PID: 785 (dockerd)
            //       Tasks: 30
            //         CPU: 4min 17.003s
            //      CGroup: /system.slice/docker.service
            //              ├─  785 /usr/bin/dockerd -H fd:// --containerd=/run/containerd/containerd.sock
            //              ├─97032 /usr/bin/docker-proxy -proto tcp -host-ip 0.0.0.0 -host-port 5000 -container-ip 172.17.0.2 -container-port 5000 -use-listen-fd
            //              └─97039 /usr/bin/docker-proxy -proto tcp -host-ip :: -host-port 5000 -container-ip 172.17.0.2 -container-port 5000 -use-listen-fd
            if (stderr?.includes('could not be found') || stderr.includes('not-found')) {
                this.adapter.log.error(`Docker is not installed: ${stderr}`);
                return false;
            }

            return stdout.includes('(running)');
        } catch {
            return false;
        }
    }

    /**
     * Ensure that the given container is running with the actual configuration
     *
     * @param container Container configuration
     */
    async #ensureActualConfiguration(container: ContainerConfig): Promise<void> {
        if (!container.name) {
            throw new Error(`Container name must be a string, but got boolean true`);
        }
        // Check the configuration of the container
        const inspect = await this.containerInspect(container.name);
        if (inspect) {
            const existingConfig = DockerManager.mapInspectToConfig(inspect);
            console.log('Compare existing config', existingConfig, ' and', container);
            container = cleanContainerConfig(container);
            const diffs = compareConfigs(container, existingConfig);
            if (diffs.length) {
                this.adapter.log.info(
                    `Configuration of own container ${container.name} has changed: ${diffs.join(
                        ', ',
                    )}. Restarting container...`,
                );
                const result = await this.containerReCreate(container);
                if (result.stderr) {
                    this.adapter.log.warn(`Cannot recreate own container ${container.name}: ${result.stderr}`);
                }
            } else {
                this.adapter.log.debug(`Configuration of own container ${container.name} is up to date`);
            }

            // Check if container is running
            const status = await this.containerList(true);
            const containerInfo = status.find(it => it.names === container.name);
            if (containerInfo) {
                if (containerInfo.status !== 'running' && containerInfo.status !== 'restarting') {
                    // Start the container
                    this.adapter.log.info(`Starting own container ${container.name}`);
                    try {
                        const result = await this.containerStart(containerInfo.id);
                        if (result.stderr) {
                            this.adapter.log.warn(`Cannot start own container ${container.name}: ${result.stderr}`);
                        }
                    } catch (e) {
                        this.adapter.log.warn(`Cannot start own container ${container.name}: ${e.message}`);
                    }
                } else {
                    this.adapter.log.debug(`Own container ${container.name} is already running`);
                }
            } else {
                this.adapter.log.warn(`Own container ${container.name} not found in container list after recreation`);
            }
        }
    }

    getDefaultContainerName(): string {
        return `iob_${this.adapter.namespace.replace(/[-.]/g, '_')}`;
    }

    async #checkOwnContainers(): Promise<void> {
        if (!this.#ownContainers.length) {
            this.#waitAllCheckedResolve?.();
            return;
        }
        const status = await this.containerList(true);
        let images = await this.imageList();
        let anyStartedOrRunning = false;
        const networkChecked: string[] = [];
        const prefix = this.getDefaultContainerName();
        for (let c = 0; c < this.#ownContainers.length; c++) {
            const container = this.#ownContainers[c];
            if (container.iobEnabled !== false) {
                if (!container.image.includes(':')) {
                    container.image += ':latest';
                }
                if (container.labels?.iobroker !== this.adapter.namespace) {
                    container.labels = { ...container.labels, iobroker: this.adapter.namespace };
                }
                container.name ||= prefix;

                // Name of the container, name of the network and name of the volume must start with iob_<Adaptername>_<instance>_
                if (container.name !== prefix && !container.name.startsWith(`${prefix}_`)) {
                    this.adapter.log.debug(`Renaming container ${container.name} to be prefixed with iob_${prefix}_`);
                    container.name = `${prefix}_${container.name}`;
                }

                try {
                    // create iobroker network if necessary
                    if (
                        container.networkMode &&
                        container.networkMode !== 'container' &&
                        container.networkMode !== 'host' &&
                        container.networkMode !== 'bridge' &&
                        container.networkMode !== 'none'
                    ) {
                        if (container.networkMode === true) {
                            container.networkMode = prefix;
                        }
                        if (container.networkMode !== prefix && !container.networkMode.startsWith(`${prefix}_`)) {
                            this.adapter.log.debug(
                                `Renaming network ${container.networkMode} to be prefixed with ${prefix}_`,
                            );
                            container.networkMode = `${prefix}_${container.networkMode}`;
                        }

                        if (!networkChecked.includes(container.networkMode)) {
                            // check if the network exists
                            const networks = await this.networkList();
                            if (!networks.find(it => it.name === container.networkMode)) {
                                this.adapter.log.info(`Creating docker network ${container.networkMode}`);
                                await this.networkCreate(container.networkMode);
                            }

                            networkChecked.push(container.networkMode);
                        }
                    }

                    // create all volumes ourselves, to have a static name
                    if (container.mounts?.find(m => m.type === 'volume')) {
                        // check if the volume exists
                        const volumes = await this.volumeList();
                        for (const mount of container.mounts) {
                            if (mount.type === 'volume' && mount.source) {
                                if (mount.source === true) {
                                    mount.source = prefix;
                                }

                                if (mount.source !== prefix && !mount.source.startsWith(`${prefix}_`)) {
                                    this.adapter.log.debug(
                                        `Renaming volume ${mount.source} to be prefixed with ${prefix}_`,
                                    );
                                    mount.source = `${prefix}_${mount.source}`;
                                }
                                if (mount.iobBackup) {
                                    if (!container.labels.iob_backup) {
                                        container.labels = { ...container.labels, iob_backup: mount.source };
                                    } else {
                                        const volumes: string[] = container.labels.iob_backup
                                            .split(',')
                                            .map(v => v.trim())
                                            .filter(v => v);
                                        if (!volumes.includes(mount.source)) {
                                            volumes.push(mount.source);
                                            container.labels = { ...container.labels, iob_backup: volumes.join(',') };
                                        }
                                    }
                                }

                                const volume = volumes.find(v => v.name === mount.source);
                                if (!volume) {
                                    this.adapter.log.info(`Creating docker volume ${mount.source}`);
                                    const result = await this.volumeCreate(mount.source);
                                    if (result.stderr) {
                                        this.adapter.log.warn(`Cannot create volume ${mount.source}: ${result.stderr}`);
                                        continue;
                                    }
                                    // Copy data from host to volume
                                    if (mount.iobAutoCopyFrom) {
                                        await this.volumeCopyTo(mount.source, mount.iobAutoCopyFrom);
                                    }
                                } else if (mount.iobAutoCopyFromForce && mount.iobAutoCopyFrom) {
                                    // Copy data from host to volume
                                    await this.volumeCopyTo(mount.source, mount.iobAutoCopyFrom);
                                }
                            }
                        }
                    }

                    let containerInfo = status.find(it => it.names === container.name);
                    let image = images.find(it => `${it.repository}:${it.tag}` === container.image);
                    if (container.iobAutoImageUpdate) {
                        // ensure that the image is actual
                        const newImage = await this.imageUpdate(container.image, true);
                        if (newImage) {
                            this.adapter.log.info(
                                `Image ${container.image} for own container ${container.name} was updated`,
                            );
                            if (containerInfo) {
                                // destroy current container
                                await this.containerRemove(containerInfo.id);
                                containerInfo = undefined;
                            }
                            image = newImage;
                        }
                    }
                    if (!image) {
                        this.adapter.log.info(`Pulling image ${container.image} for own container ${container.name}`);
                        try {
                            const result = await this.imagePull(container.image);
                            if (result.stderr) {
                                this.adapter.log.warn(`Cannot pull image ${container.image}: ${result.stderr}`);
                                continue;
                            }
                        } catch (e) {
                            this.adapter.log.warn(`Cannot pull image ${container.image}: ${e.message}`);
                            continue;
                        }
                        // Check that image is available now
                        images = await this.imageList();
                        image = images.find(it => `${it.repository}:${it.tag}` === container.image);
                        if (!image) {
                            this.adapter.log.warn(
                                `Image ${container.image} for own container ${container.name} not found after pull`,
                            );
                            continue;
                        }
                    }

                    if (containerInfo) {
                        await this.#ensureActualConfiguration(container);
                        anyStartedOrRunning ||= !!container.iobMonitoringEnabled;
                    } else {
                        // Create and start the container, as the container was not found
                        this.adapter.log.info(`Creating and starting own container ${container.name}`);

                        try {
                            const result = await this.containerRun(container);
                            if (result.stderr) {
                                this.adapter.log.warn(`Cannot start own container ${container.name}: ${result.stderr}`);
                            } else {
                                anyStartedOrRunning ||= !!container.iobMonitoringEnabled;
                            }
                        } catch (e) {
                            this.adapter.log.warn(`Cannot start own container ${container.name}: ${e.message}`);
                        }
                    }
                } catch (e) {
                    this.adapter.log.warn(`Cannot check own container ${container.name}: ${e.message}`);
                }
            }
        }

        if (anyStartedOrRunning) {
            this.#monitoringInterval ||= setInterval(() => this.#monitorOwnContainers(), 60000);
        }
        this.#waitAllCheckedResolve?.();
    }

    allOwnContainersChecked(): Promise<void> {
        return this.#waitAllChecked;
    }

    async #monitorOwnContainers(): Promise<void> {
        // get the status of containers
        const containers = await this.containerList();
        // Check the status of own containers
        for (let c = 0; c < this.#ownContainers.length; c++) {
            const container = this.#ownContainers[c];
            if (container.iobEnabled !== false && container.iobMonitoringEnabled && container.name) {
                // Check if container is running
                const running = containers.find(it => it.names === container.name);
                if (!running || (running.status !== 'running' && running.status !== 'restarting')) {
                    this.adapter.log.warn(`Own container ${container.name} is not running. Restarting...`);
                    try {
                        const result = await this.containerStart(container.name);
                        if (result.stderr) {
                            this.adapter.log.warn(`Cannot start own container ${container.name}: ${result.stderr}`);
                            this.#ownContainersStats[container.name] = {
                                ...this.#ownContainersStats[container.name],
                                status: running?.status || 'unknown',
                                statusTs: Date.now(),
                            };
                            continue;
                        }
                    } catch (e) {
                        this.adapter.log.warn(`Cannot start own container ${container.name}: ${e.message}`);
                        this.#ownContainersStats[container.name] = {
                            ...this.#ownContainersStats[container.name],
                            status: running?.status || 'unknown',
                            statusTs: Date.now(),
                        };
                        continue;
                    }
                }

                // check the stats
                this.#ownContainersStats[container.name] = {
                    ...((await this.containerGetRamAndCpuUsage(container.name)) || ({} as ContainerStats)),
                    status: running?.status || 'unknown',
                    statusTs: Date.now(),
                };
            }
        }
    }

    /** Read own container stats */
    getOwnContainerStats(): { [name: string]: ContainerStatus } {
        return this.#ownContainersStats;
    }

    async containerGetRamAndCpuUsage(containerNameOrId: ContainerName): Promise<ContainerStats | null> {
        try {
            const { stdout } = await this.#exec(
                `stats ${containerNameOrId} --no-stream --format "{{.CPUPerc}};{{.MemUsage}};{{.NetIO}};{{.BlockIO}};{{.PIDs}}"`,
            );
            // Example: "0.15%;12.34MiB / 512MiB;1.2kB / 2.3kB;0B / 0B;5"
            const [cpuStr, memStr, netStr, blockIoStr, pid] = stdout.trim().split(';');
            const [memUsed, memMax] = memStr.split('/').map(it => it.trim());
            const [netRead, netWrite] = netStr.split('/').map(it => it.trim());
            const [blockIoRead, blockIoWrite] = blockIoStr.split('/').map(it => it.trim());

            return {
                ts: Date.now(),
                cpu: parseFloat(cpuStr.replace('%', '').replace(',', '.')),
                memUsed: this.#parseSize(memUsed.replace('iB', 'B')),
                memMax: this.#parseSize(memMax.replace('iB', 'B')),
                netRead: this.#parseSize(netRead.replace('iB', 'B')),
                netWrite: this.#parseSize(netWrite.replace('iB', 'B')),
                processes: parseInt(pid, 10),
                blockIoRead: this.#parseSize(blockIoRead.replace('iB', 'B')),
                blockIoWrite: this.#parseSize(blockIoWrite.replace('iB', 'B')),
            };
        } catch (e) {
            this.adapter.log.debug(`Cannot get stats: ${e.message}`);
            return null;
        }
    }

    /**
     * Update the image if a newer version is available
     *
     * @param image Image name with tag
     * @param ignoreIfNotExist If true, do not throw error if image does not exist
     * @returns New image info if image was updated, null if no update was necessary
     */
    async imageUpdate(image: ImageName, ignoreIfNotExist?: boolean): Promise<ImageInfo | null> {
        const list = await this.imageList();
        if (!image.includes(':')) {
            image += ':latest';
        }
        const existingImage = list.find(it => `${it.repository}:${it.tag}` === image);
        if (!existingImage && !ignoreIfNotExist) {
            throw new Error(`Image ${image} not found`);
        }
        // Pull the image
        const result = await this.imagePull(image);
        if (result.stderr) {
            throw new Error(`Cannot pull image ${image}: ${result.stderr}`);
        }
        const newList = await this.imageList();
        const newImage = newList.find(it => `${it.repository}:${it.tag}` === image);
        if (!newImage) {
            throw new Error(`Image ${image} not found after pull`);
        }
        // If image ID has changed, image was updated
        return !existingImage || existingImage.id !== newImage.id ? newImage : null;
    }

    #exec(command: string): Promise<{ stdout: string; stderr: string }> {
        if (!this.installed) {
            return Promise.reject(new Error('Docker is not installed'));
        }
        const finalCommand = this.needSudo ? `sudo docker ${command}` : `docker ${command}`;
        return execPromise(finalCommand);
    }

    async #isDockerInstalled(): Promise<string | false> {
        if (this.#driver === 'cli') {
            try {
                const result = await execPromise('docker --version');
                if (!result.stderr && result.stdout) {
                    // "Docker version 28.3.2, build 578ccf6\n"
                    return result.stdout.split('\n')[0].trim();
                }
                this.adapter.log.debug(`Docker not installed: ${result.stderr}`);
            } catch (e) {
                this.adapter.log.debug(`Docker not installed: ${e.message}`);
            }
        } else if (this.#dockerode) {
            try {
                const info = await this.#dockerode.version();
                if (info?.Version) {
                    return `Docker version ${info.Version}, api version: ${info.ApiVersion}`;
                }
            } catch (e) {
                this.adapter.log.debug(`Docker not installed: ${e.message}`);
            }
        }
        return false;
    }

    async #isNeedSudo(): Promise<boolean> {
        try {
            await execPromise('docker ps');
            return false;
        } catch {
            return true;
        }
    }

    /** Get disk usage information */
    async discUsage(): Promise<DiskUsage> {
        if (this.#dockerode) {
            const info = await this.#dockerode.df();
            const result: DiskUsage = { total: { size: 0, reclaimable: 0 } };

            if (info.Images) {
                let size = 0;
                let reclaimable = 0;
                for (const image of info.Images) {
                    size += image.Size;
                    reclaimable += image.SharedSize + image.VirtualSize;
                }
                result.images = {
                    total: info.Images.length,
                    // @ts-expect-error todo
                    active: info.Images.filter(img => img.Containers > 0).length,
                    size,
                    reclaimable,
                };
                result.total.size += size;
                result.total.reclaimable += reclaimable;
            }

            if (info.Containers) {
                let size = 0;
                for (const container of info.Containers) {
                    size += container.SizeRootFs || 0;
                }
                result.containers = {
                    total: info.Containers.length,
                    // @ts-expect-error todo
                    active: info.Containers.filter(cont => cont.State === 'running').length,
                    size,
                    reclaimable: 0, // Not available
                };
                result.total.size += size;
            }

            if (info.Volumes) {
                let size = 0;
                for (const volume of info.Volumes) {
                    size += volume.UsageData?.Size || 0;
                }
                result.volumes = {
                    total: info.Volumes.length,
                    // @ts-expect-error todo
                    active: info.Volumes.filter(vol => vol.UsageData?.RefCount && vol.UsageData.RefCount > 0).length,
                    size,
                    reclaimable: 0, // Not available
                };
                result.total.size += size;
            }

            // Build cache not available

            return result;
        }
        const { stdout } = await this.#exec(`system df`);
        const result: DiskUsage = { total: { size: 0, reclaimable: 0 } };
        // parse the output
        // TYPE            TOTAL     ACTIVE    SIZE      RECLAIMABLE
        // Images          2         1         2.715GB   2.715GB (99%)
        // Containers      1         1         26.22MB   0B (0%)
        // Local Volumes   0         0         0B        0B
        // Build Cache     0         0         0B        0B
        const lines = stdout.split('\n');
        for (const line of lines) {
            const parts = line.trim().split(/\s+/);
            if (parts.length >= 5 && parts[0] !== 'TYPE') {
                let size: number | undefined;
                let reclaimable: number | undefined;

                if (parts[0] === 'Images') {
                    const sizeStr = parts[3];
                    const reclaimableStr = parts[4].split(' ')[0];
                    size = this.#parseSize(sizeStr);
                    reclaimable = this.#parseSize(reclaimableStr);
                    result.images = {
                        total: parseInt(parts[1], 10),
                        active: parseInt(parts[2], 10),
                        size,
                        reclaimable: reclaimable,
                    };
                } else if (parts[0] === 'Containers') {
                    const sizeStr = parts[3];
                    const reclaimableStr = parts[4].split(' ')[0];
                    size = this.#parseSize(sizeStr);
                    reclaimable = this.#parseSize(reclaimableStr);
                    result.containers = {
                        total: parseInt(parts[1], 10),
                        active: parseInt(parts[2], 10),
                        size,
                        reclaimable: reclaimable,
                    };
                } else if (parts[0] === 'Local' && parts[1] === 'Volumes') {
                    const sizeStr = parts[4];
                    const reclaimableStr = parts[5].split(' ')[0];
                    size = this.#parseSize(sizeStr);
                    reclaimable = this.#parseSize(reclaimableStr);
                    result.volumes = {
                        total: parseInt(parts[2], 10),
                        active: parseInt(parts[3], 10),
                        size,
                        reclaimable: reclaimable,
                    };
                } else if (parts[0] === 'Build' && parts[1] === 'Cache') {
                    const sizeStr = parts[4];
                    const reclaimableStr = parts[5].split(' ')[0];
                    size = this.#parseSize(sizeStr);
                    reclaimable = this.#parseSize(reclaimableStr);
                    result.buildCache = {
                        total: parseInt(parts[2], 10),
                        active: parseInt(parts[3], 10),
                        size,
                        reclaimable: reclaimable,
                    };
                }
                result.total.size += size || 0;
                result.total.reclaimable += reclaimable || 0;
            }
        }
        return result;
    }

    /** Pull an image from the registry */
    async imagePull(image: ImageName): Promise<{ stdout: string; stderr: string; images?: ImageInfo[] }> {
        if (!image.includes(':')) {
            image += ':latest';
        }
        if (this.#dockerode) {
            const stream = await this.#dockerode.pull(image);
            if (!stream) {
                throw new Error('No stream returned');
            }
            return new Promise<{ stdout: string; stderr: string; images?: ImageInfo[] }>((resolve, reject) => {
                const onFinished = (err: Error | null): void => {
                    if (err) {
                        return reject(err);
                    }
                    this.imageList()
                        .then(images => resolve({ stdout: `Image ${image} pulled`, stderr: '', images }))
                        .catch(reject);
                };
                const lastShownProgress: { [id: string]: number } = {};
                const onProgress = (
                    event:
                        | {
                              status: 'Downloading';
                              progressDetail: { current: number; total: number };
                              progress: string;
                              id: string;
                          }
                        | {
                              status: 'Download complete' | 'Verifying Checksum' | 'Pull complete';
                              id: string;
                          }
                        | {
                              status: 'Extracting';
                              progressDetail: { current: number; total: number };
                              progress: string;
                              id: string;
                          },
                ): void => {
                    // {"status":"Downloading","progressDetail":{"current":109494080,"total":689664036},"progress":"[=======> ]  109.5MB/689.7MB","id":"29bce3058cea"}
                    // {"status":"Download complete","progressDetail":{},"id":"6859c690a072"}
                    // {"status":"Verifying Checksum","progressDetail":{},"id":"6859c690a072"}
                    // {"status":"Extracting","progressDetail":{"current":32,"total":32},"progress":"[======>] 32B/32B","id":"4f4fb700ef54"}
                    // {"status":"Pull complete","progressDetail":{},"id":"4f4fb700ef54"}

                    if (!lastShownProgress || Date.now() - lastShownProgress[event.id] > 4000) {
                        if (
                            event.status === 'Download complete' ||
                            event.status === 'Pull complete' ||
                            event.status === 'Verifying Checksum'
                        ) {
                            this.adapter.log.debug(`Image ${image}/${event.id}: ${event.status}`);
                        } else if (event.status === 'Downloading' || event.status === 'Extracting') {
                            this.adapter.log.debug(
                                `Pulling image ${image}/${event.id}: ${event.status} ${Math.round((event.progressDetail.current / event.progressDetail.total) * 1000) / 10}% of ${size2string(event.progressDetail.total)}`,
                            );
                        } else {
                            this.adapter.log.debug(`Pulling image ${image}/${event.id}: ${JSON.stringify(event)}`);
                        }
                        lastShownProgress[event.id] = Date.now();
                    }
                };
                this.#dockerode!.modem.followProgress(stream, onFinished, onProgress);
            });
        }
        try {
            const result = await this.#exec(`pull ${image}`);
            const images = await this.imageList();
            if (!images.find(it => `${it.repository}:${it.tag}` === image)) {
                throw new Error(`Image ${image} not found after pull`);
            }
            return { ...result, images };
        } catch (e) {
            return { stdout: '', stderr: e.message.toString() };
        }
    }

    /** Autocomplete image names from Docker Hub */
    async imageNameAutocomplete(partialName: string): Promise<
        {
            name: string;
            description: string;
            isOfficial: boolean;
            starCount: number;
        }[]
    > {
        try {
            // Read stars and descriptions
            const { stdout } = await this.#exec(
                `search ${partialName} --format "{{.Name}};{{.Description}};{{.IsOfficial}};{{.StarCount}}" --limit 50`,
            );
            return stdout
                .split('\n')
                .filter(line => line.trim() !== '')
                .map(line => {
                    const [name, description, isOfficial, starCount] = line.split(';');
                    return {
                        name,
                        description,
                        isOfficial: isOfficial === 'true',
                        starCount: parseInt(starCount, 10) || 0,
                    };
                });
        } catch (e) {
            this.adapter.log.debug(`Cannot search images: ${e.message}`);
            return [];
        }
    }

    static getDockerodeConfig(config: ContainerConfig): Docker.ContainerCreateOptions {
        let mounts: MountSettings[] | undefined;
        if (config.mounts) {
            for (const mount of config.mounts) {
                let volumeOptions:
                    | {
                          NoCopy: boolean;
                          Labels: { [label: string]: string };
                          DriverConfig: {
                              Name: string;
                              Options: { [option: string]: string };
                          };
                          Subpath?: string;
                      }
                    | undefined;
                if (mount.volumeOptions) {
                    if (mount.volumeOptions.nocopy !== undefined) {
                        volumeOptions ||= {} as {
                            NoCopy: boolean;
                            Labels: { [label: string]: string };
                            DriverConfig: {
                                Name: string;
                                Options: { [option: string]: string };
                            };
                            Subpath?: string;
                        };
                        volumeOptions.NoCopy = mount.volumeOptions.nocopy;
                    }
                    if (mount.volumeOptions.labels) {
                        volumeOptions ||= {} as {
                            NoCopy: boolean;
                            Labels: { [label: string]: string };
                            DriverConfig: {
                                Name: string;
                                Options: { [option: string]: string };
                            };
                            Subpath?: string;
                        };
                        volumeOptions.Labels = mount.volumeOptions.labels;
                    }
                }
                let bindOptions:
                    | {
                          Propagation: MountPropagation;
                      }
                    | undefined;
                if (mount.bindOptions) {
                    if (mount.bindOptions.propagation) {
                        bindOptions ||= {} as {
                            Propagation: MountPropagation;
                        };
                        bindOptions.Propagation = mount.bindOptions.propagation;
                    }
                }

                let tmpfsOptions:
                    | {
                          SizeBytes: number;
                          Mode: number;
                      }
                    | undefined;
                if (mount.tmpfsOptions) {
                    if (mount.tmpfsOptions.size !== undefined) {
                        tmpfsOptions ||= {} as {
                            SizeBytes: number;
                            Mode: number;
                        };
                        tmpfsOptions.SizeBytes = mount.tmpfsOptions.size;
                    }
                    if (mount.tmpfsOptions.mode !== undefined) {
                        tmpfsOptions ||= {} as {
                            SizeBytes: number;
                            Mode: number;
                        };
                        tmpfsOptions.Mode = mount.tmpfsOptions.mode;
                    }
                }
                if (mount.source === true) {
                    throw new Error(`Mount source must be a string, but got boolean true`);
                }

                const m: MountSettings = {
                    Target: mount.target,
                    Source: mount.source || '',
                    Type: mount.type as MountType,
                    ReadOnly: mount.readOnly,
                    Consistency: mount.consistency,
                    VolumeOptions: volumeOptions,
                    BindOptions: bindOptions,
                    TmpfsOptions: tmpfsOptions,
                };
                mounts ||= [];
                mounts.push(m);
            }
        }
        if (!config.name) {
            throw new Error(`Container name must be a string, but got boolean true`);
        }

        return {
            name: config.name,
            Image: config.image,
            Cmd: Array.isArray(config.command)
                ? config.command
                : typeof config.command === 'string'
                  ? [config.command]
                  : undefined,
            Entrypoint: config.entrypoint,
            Env: config.environment
                ? Object.keys(config.environment).map(key => `${key}=${config.environment![key]}`)
                : undefined,
            // WorkingDir: config.workingDir,
            // { '/data': {} }
            Volumes: config.volumes?.reduce((acc, vol) => (acc[vol] = {}), {} as { [key: string]: object }),
            Labels: config.labels,
            ExposedPorts: config.ports
                ? config.ports.reduce(
                      (acc, port) => {
                          acc[`${port.containerPort}/${port.protocol || 'tcp'}`] = {};
                          return acc;
                      },
                      {} as { [key: string]: object },
                  )
                : undefined,
            HostConfig: {
                // Binds: config.binds,
                PortBindings: config.ports
                    ? config.ports.reduce(
                          (acc, port) => {
                              acc[`${port.containerPort}/${port.protocol || 'tcp'}`] = [
                                  {
                                      HostPort: port.hostPort ? port.hostPort.toString() : undefined,
                                      HostIp: port.hostIP || undefined,
                                  },
                              ];
                              return acc;
                          },
                          {} as { [key: string]: Array<{ HostPort?: string; HostIp?: string }> },
                      )
                    : undefined,
                Mounts: mounts,
                NetworkMode: config.networkMode === true ? '' : config.networkMode || undefined,
                // Links: config.links,
                // Dns: config.dns,
                // DnsOptions: config.dnsOptions,
                // DnsSearch: config.dnsSearch,
                ExtraHosts: config.extraHosts,
                // VolumesFrom: config.volumesFrom,
                Privileged: config.security?.privileged,
                CapAdd: config.security?.capAdd,
                CapDrop: config.security?.capDrop,
                UsernsMode: config.security?.usernsMode,
                IpcMode: config.security?.ipc,
                PidMode: config.security?.pid,
                GroupAdd: config.security?.groupAdd?.map(g => g.toString()),
                ReadonlyRootfs: config.readOnly,
                RestartPolicy: {
                    Name: config.restart?.policy || 'no',
                    MaximumRetryCount: config.restart?.maxRetries || 0,
                },
                CpuShares: config.resources?.cpuShares,
                CpuPeriod: config.resources?.cpuPeriod,
                CpuQuota: config.resources?.cpuQuota,
                CpusetCpus: config.resources?.cpus?.toString(),
                Memory: config.resources?.memory,
                MemorySwap: config.resources?.memorySwap,
                MemoryReservation: config.resources?.memoryReservation,
                // OomKillDisable: config.resources?.oomKillDisable,
                // OomScoreAdj: config.resources?.oomScoreAdj,
                LogConfig: config.logging
                    ? {
                          Type: config.logging.driver || 'json-file',
                          Config: config.logging.options || {},
                      }
                    : undefined,
                SecurityOpt: [
                    ...(config.security?.seccomp ? [`seccomp=${config.security.seccomp}`] : []),
                    ...(config.security?.apparmor ? [`apparmor=${config.security.apparmor}`] : []),
                    ...(config.security?.noNewPrivileges ? ['no-new-privileges:true'] : []),
                ],
                Sysctls: config.sysctls,
                Init: config.init,
            },
            StopSignal: config.stop?.signal,
            StopTimeout: config.stop?.gracePeriodSec,
            Tty: config.tty,
            OpenStdin: config.openStdin,
        };
    }

    /**
     * Create and start a container with the given configuration. No checks are done.
     */
    async containerRun(config: ContainerConfig): Promise<{ stdout: string; stderr: string }> {
        if (this.#dockerode) {
            const container = await this.#dockerode.createContainer(DockerManager.getDockerodeConfig(config));
            await container.start();
            return { stdout: `Container ${config.name} started`, stderr: '' };
        }

        try {
            return await this.#exec(`run ${DockerManager.toDockerRun(config)}`);
        } catch (e) {
            return { stdout: '', stderr: e.message.toString() };
        }
    }

    /**
     * Create a container with the given configuration without starting it. No checks are done.
     */
    async containerCreate(config: ContainerConfig): Promise<{ stdout: string; stderr: string }> {
        if (this.#dockerode) {
            const container = await this.#dockerode.createContainer(DockerManager.getDockerodeConfig(config));
            return { stdout: `Container ${container.id} created`, stderr: '' };
        }
        try {
            return await this.#exec(`create ${DockerManager.toDockerRun(config, true)}`);
        } catch (e) {
            return { stdout: '', stderr: e.message.toString() };
        }
    }

    /**
     * Recreate a container
     *
     * This function checks if a container is running, stops it if necessary,
     * removes it and creates a new one with the given configuration.
     * The container is not started after creation.
     *
     * @param config new configuration
     * @returns stdout and stderr of the create command
     */
    async containerReCreate(config: ContainerConfig): Promise<{ stdout: string; stderr: string }> {
        if (this.#dockerode) {
            // Get if the container is running
            let containers = await this.containerList();
            // find ID of container
            const containerInfo = containers.find(it => it.names === config.name);
            if (containerInfo) {
                const container = this.#dockerode.getContainer(containerInfo.id);
                if (containerInfo.status === 'running' || containerInfo.status === 'restarting') {
                    await container.stop();
                    containers = await this.containerList();

                    if (containers.find(it => it.id === containerInfo.id && it.status === 'running')) {
                        this.adapter.log.warn(`Cannot remove container: still running`);
                        throw new Error(`Container ${containerInfo.id} still running after stop`);
                    }
                }
                // Remove container
                await container.remove();

                containers = await this.containerList();
                if (containers.find(it => it.id === containerInfo.id)) {
                    this.adapter.log.warn(`Cannot remove container: still existing`);
                    throw new Error(`Container ${containerInfo.id} still found after remove`);
                }
            }
            const newContainer = await this.#dockerode.createContainer(DockerManager.getDockerodeConfig(config));
            return { stdout: `Container ${newContainer.id} created`, stderr: '' };
        }
        try {
            // Get if the container is running
            let containers = await this.containerList();
            // find ID of container
            const containerInfo = containers.find(it => it.names === config.name);
            if (containerInfo) {
                if (containerInfo.status === 'running' || containerInfo.status === 'restarting') {
                    const stopResult = await this.#exec(`stop ${containerInfo.id}`);
                    containers = await this.containerList();

                    if (containers.find(it => it.id === containerInfo.id && it.status === 'running')) {
                        this.adapter.log.warn(`Cannot remove container: ${stopResult.stderr || stopResult.stdout}`);
                        throw new Error(`Container ${containerInfo.id} still running after stop`);
                    }
                }
                // Remove container
                const rmResult = await this.#exec(`rm ${containerInfo.id}`);

                containers = await this.containerList();
                if (containers.find(it => it.id === containerInfo.id)) {
                    this.adapter.log.warn(`Cannot remove container: ${rmResult.stderr || rmResult.stdout}`);
                    throw new Error(`Container ${containerInfo.id} still found after remove`);
                }
            }
            return await this.#exec(`create ${DockerManager.toDockerRun(config, true)}`);
        } catch (e) {
            return { stdout: '', stderr: e.message.toString() };
        }
    }

    async containerCreateCompose(compose: string): Promise<{ stdout: string; stderr: string }> {
        try {
            return await this.#exec(`compose -f ${compose} create`);
        } catch (e) {
            return { stdout: '', stderr: e.message.toString() };
        }
    }

    /** List all images */
    async imageList(): Promise<ImageInfo[]> {
        if (this.#dockerode) {
            const images = await this.#dockerode.listImages();
            return images.map(img => {
                const repoTag = img.RepoTags && img.RepoTags.length ? img.RepoTags[0] : '<none>:<none>';
                const [repository, tag] = repoTag.split(':');
                return {
                    repository,
                    tag,
                    id: img.Id.startsWith('sha256:') ? img.Id.substring(7, 19) : img.Id.substring(0, 12),
                    createdSince: new Date(img.Created * 1000).toISOString(),
                    size: img.Size,
                };
            });
        }
        try {
            const { stdout } = await this.#exec(
                'images --format "{{.Repository}}:{{.Tag}};{{.ID}};{{.CreatedAt}};{{.Size}}"',
            );
            return stdout
                .split('\n')
                .filter(line => line.trim() !== '')
                .map(line => {
                    const [repositoryTag, id, createdSince, size] = line.split(';');
                    const [repository, tag] = repositoryTag.split(':');
                    return {
                        repository,
                        tag,
                        id,
                        createdSince,
                        size: this.#parseSize(size),
                    };
                });
        } catch (e) {
            this.adapter.log.debug(`Cannot list images: ${e.message}`);
            return [];
        }
    }

    /** Build an image from a Dockerfile */
    async imageBuild(dockerfilePath: string, tag: string): Promise<{ stdout: string; stderr: string }> {
        if (this.#dockerode) {
            try {
                const stream = await this.#dockerode.buildImage(dockerfilePath, {
                    t: tag,
                    dockerfile: dockerfilePath,
                });

                return new Promise<{ stdout: string; stderr: string }>((resolve, reject) => {
                    let stdout = '';
                    let stderr = '';

                    const onFinished = (err: Error | null): void => {
                        if (err) {
                            return reject(err);
                        }
                        resolve({ stdout, stderr });
                    };
                    const onProgress = (event: any): void => {
                        if (event.stream) {
                            stdout += event.stream;
                        }
                        if (event.error) {
                            stderr += event.error;
                        }
                        this.adapter.log.debug(JSON.stringify(event));
                    };
                    this.#dockerode!.modem.followProgress(stream, onFinished, onProgress);
                });
            } catch (e) {
                return { stdout: '', stderr: e.message.toString() };
            }
        }
        try {
            return await this.#exec(`build -t ${tag} -f ${dockerfilePath} .`);
        } catch (e) {
            return { stdout: '', stderr: e.message.toString() };
        }
    }

    /** Tag an image with a new tag */
    async imageTag(imageId: ImageName, newTag: string): Promise<{ stdout: string; stderr: string }> {
        try {
            return await this.#exec(`tag ${imageId} ${newTag}`);
        } catch (e) {
            return { stdout: '', stderr: e.message.toString() };
        }
    }

    /** Remove an image */
    async imageRemove(imageId: ImageName): Promise<{ stdout: string; stderr: string; images?: ImageInfo[] }> {
        try {
            const result = await this.#exec(`rmi ${imageId}`);
            const images = await this.imageList();
            if (images.find(it => `${it.repository}:${it.tag}` === imageId)) {
                return { stdout: '', stderr: `Image ${imageId} still found after deletion`, images };
            }
            return { ...result, images };
        } catch (e) {
            return { stdout: '', stderr: e.message.toString() };
        }
    }

    static rodeInspect2DockerImageInspect(data: Docker.ImageInspectInfo): DockerImageInspect {
        return {
            Id: data.Id.startsWith('sha256:') ? data.Id.substring(7, 19) : data.Id.substring(0, 12),
            RepoTags: data.RepoTags,
            RepoDigests: data.RepoDigests,
            Parent: data.Parent,
            Comment: data.Comment,
            Created: data.Created,
            DockerVersion: data.DockerVersion,
            Author: data.Author,
            Architecture: data.Architecture,
            Os: data.Os,
            Size: data.Size,
            GraphDriver: {
                Data: data.GraphDriver.Data as any,
                Name: data.GraphDriver.Name,
            },
            RootFS: data.RootFS,
            Config: {
                ...data.Config,
                Entrypoint: Array.isArray(data.Config.Entrypoint)
                    ? data.Config.Entrypoint
                    : typeof data.Config.Entrypoint === 'string'
                      ? [data.Config.Entrypoint]
                      : [],
            },
        };
    }

    /** Inspect an image */
    async imageInspect(imageId: ImageName): Promise<DockerImageInspect | null> {
        if (this.#dockerode) {
            const image = this.#dockerode.getImage(imageId);
            const data = await image.inspect();
            return DockerManager.rodeInspect2DockerImageInspect(data);
        }

        try {
            const { stdout } = await this.#exec(`inspect ${imageId}`);
            return JSON.parse(stdout)[0];
        } catch (e) {
            this.adapter.log.debug(`Cannot inspect image: ${e.message.toString()}`);
            return null;
        }
    }

    async imagePrune(): Promise<{ stdout: string; stderr: string }> {
        if (this.#dockerode) {
            try {
                await this.#dockerode.pruneImages();
                return { stdout: 'Unused images pruned', stderr: '' };
            } catch (e) {
                return { stdout: '', stderr: e.message.toString() };
            }
        }
        try {
            return await this.#exec(`image prune -f`);
        } catch (e) {
            return { stdout: '', stderr: e.message.toString() };
        }
    }

    #parseSize(sizeStr: string): number {
        const units: { [key: string]: number } = {
            B: 1,
            KB: 1024,
            MB: 1024 * 1024,
            GB: 1024 * 1024 * 1024,
            TB: 1024 * 1024 * 1024 * 1024,
        };
        const match = sizeStr.match(/^([\d.]+)([KMGTP]?B)$/);
        if (match) {
            const value = parseFloat(match[1]);
            const unit = match[2];
            return value * (units[unit] || 1);
        }
        return 0;
    }

    /**
     * Stop a container
     *
     * @param container Container name or ID
     */
    async containerStop(
        container: ContainerName,
    ): Promise<{ stdout: string; stderr: string; containers?: ContainerInfo[] }> {
        if (this.#dockerode) {
            let containers = await this.containerList();
            // find ID of container
            const containerInfo = containers.find(it => it.names === container || it.id === container);
            if (!containerInfo) {
                throw new Error(`Container ${container} not found`);
            }
            const dockerContainer = this.#dockerode.getContainer(containerInfo.id);
            await dockerContainer.stop();
            containers = await this.containerList();
            if (containers.find(it => it.id === containerInfo.id && it.status === 'running')) {
                throw new Error(`Container ${container} still running after stop`);
            }
            return { stdout: `Contained ${containerInfo.id} stopped`, stderr: '', containers };
        }

        try {
            let containers = await this.containerList();
            // find ID of container
            const containerInfo = containers.find(it => it.names === container || it.id === container);
            if (!containerInfo) {
                throw new Error(`Container ${container} not found`);
            }

            const result = await this.#exec(`stop ${containerInfo.id}`);
            containers = await this.containerList();
            if (containers.find(it => it.id === containerInfo.id && it.status === 'running')) {
                throw new Error(`Container ${container} still running after stop`);
            }
            return { ...result, containers };
        } catch (e) {
            return { stdout: '', stderr: e.message.toString() };
        }
    }

    /**
     * Start a container
     *
     * @param container Container name or ID
     */
    async containerStart(
        container: ContainerName,
    ): Promise<{ stdout: string; stderr: string; containers?: ContainerInfo[] }> {
        if (this.#dockerode) {
            let containers = await this.containerList();
            // find ID of container
            const containerInfo = containers.find(it => it.names === container || it.id === container);
            if (!containerInfo) {
                throw new Error(`Container ${container} not found`);
            }
            const dockerContainer = this.#dockerode.getContainer(containerInfo.id);
            await dockerContainer.start();
            containers = await this.containerList();
            if (
                containers.find(
                    it => it.id === containerInfo.id && it.status !== 'running' && it.status !== 'restarting',
                )
            ) {
                throw new Error(`Container ${container} still running after stop`);
            }
            return { stdout: `Container ${containerInfo.id} started`, stderr: '', containers };
        }
        try {
            let containers = await this.containerList();
            // find ID of container
            const containerInfo = containers.find(it => it.names === container || it.id === container);
            if (!containerInfo) {
                throw new Error(`Container ${container} not found`);
            }

            const result = await this.#exec(`start ${containerInfo.id}`);
            containers = await this.containerList();
            if (
                containers.find(
                    it => it.id === containerInfo.id && it.status !== 'running' && it.status !== 'restarting',
                )
            ) {
                throw new Error(`Container ${container} still running after stop`);
            }
            return { ...result, containers };
        } catch (e) {
            return { stdout: '', stderr: e.message.toString() };
        }
    }

    /**
     * Restart a container
     *
     * This function restarts a container by its name or ID.
     * It accepts an optional timeout in seconds to wait before killing the container (default is 5 seconds).
     *
     * @param container Container name or ID
     * @param timeoutSeconds Timeout in seconds to wait before killing the container (default: 5)
     */
    async containerRestart(
        container?: ContainerName,
        timeoutSeconds?: number,
    ): Promise<{ stdout: string; stderr: string }> {
        container ||= this.getDefaultContainerName();
        if (this.#dockerode) {
            const containers = await this.containerList();
            // find ID of container
            const containerInfo = containers.find(it => it.names === container || it.id === container);
            if (!containerInfo) {
                throw new Error(`Container ${container} not found`);
            }
            const dockerContainer = this.#dockerode.getContainer(containerInfo.id);
            await dockerContainer.restart({ t: timeoutSeconds || 5 });
            return { stdout: `Container ${containerInfo.id} restarted`, stderr: '' };
        }
        try {
            const containers = await this.containerList();
            // find ID of container
            const containerInfo = containers.find(it => it.names === container || it.id === container);
            if (!containerInfo) {
                throw new Error(`Container ${container} not found`);
            }

            return await this.#exec(`restart -t ${timeoutSeconds || 5} ${containerInfo.id}`);
        } catch (e) {
            return { stdout: '', stderr: e.message.toString() };
        }
    }

    /** Find the IP address of a container, via which it can be reached from the host */
    async getIpOfContainer(containerName?: ContainerName): Promise<string> {
        containerName ||= this.getDefaultContainerName();
        const data = await this.containerInspect(containerName);
        if (!data?.NetworkSettings?.Networks) {
            throw new Error(`No network settings found for container ${containerName}`);
        }
        for (const n in data.NetworkSettings.Networks) {
            if (data.NetworkSettings.Networks[n].IPAddress) {
                return data.NetworkSettings.Networks[n].IPAddress;
            }
        }
        throw new Error(`No IP address found for container ${containerName}`);
    }

    /**
     * Remove the container and if necessary, stop it first
     *
     * @param container Container name or ID
     */
    async containerRemove(
        container: ContainerName,
    ): Promise<{ stdout: string; stderr: string; containers?: ContainerInfo[] }> {
        try {
            let containers = await this.containerList();
            // find ID of container
            const containerInfo = containers.find(it => it.names === container || it.id === container);
            if (!containerInfo) {
                throw new Error(`Container ${container} not found`);
            }
            // ensure that container is stopped
            if (containerInfo.status === 'running' || containerInfo.status === 'restarting') {
                // stop container
                const result = await this.#exec(`stop ${containerInfo.id}`);
                if (result.stderr) {
                    throw new Error(`Cannot stop container ${container}: ${result.stderr}`);
                }
            }

            const result = await this.#exec(`rm ${container}`);

            containers = await this.containerList();
            if (containers.find(it => it.id === containerInfo.id)) {
                throw new Error(`Container ${container} still found after stop`);
            }
            return { ...result, containers };
        } catch (e) {
            return { stdout: '', stderr: e.message.toString() };
        }
    }

    /**
     * List all containers
     *
     * @param all If true, list all containers. If false, list only running containers. Default is true.
     */
    async containerList(all: boolean = true): Promise<ContainerInfo[]> {
        if (this.#dockerode) {
            const containers = await this.#dockerode.listContainers({ all });
            return containers.map(cont => {
                const statusKey: string = cont.State.toLowerCase();
                let status: ContainerInfo['status'];
                if (statusKey === 'up') {
                    status = 'running';
                } else if (statusKey === 'exited') {
                    status = 'exited';
                } else if (statusKey === 'created') {
                    status = 'created';
                } else if (statusKey === 'paused') {
                    status = 'paused';
                } else if (statusKey === 'restarting') {
                    status = 'restarting';
                } else {
                    status = statusKey as ContainerInfo['status'];
                }

                // Try to convert: Up 6 minutes, Up 6 hours, Up 6 days to minutes
                const uptimeMatch = cont.Status.match(/Up (\d+) (seconds?|minutes?|hours?|days?)/);
                let minutes = 0;
                if (uptimeMatch) {
                    const value = parseInt(uptimeMatch[1], 10);
                    const unit = uptimeMatch[2];
                    if (unit.startsWith('hour')) {
                        minutes = value * 60;
                    } else if (unit.startsWith('day')) {
                        minutes = value * 60 * 24;
                    } else if (unit.startsWith('second')) {
                        minutes = Math.ceil(value / 60);
                    }
                } else if (cont.Status === 'Up About a minute') {
                    minutes = 1;
                }
                return {
                    id: cont.Id.substring(0, 12),
                    image: cont.Image,
                    command: cont.Command,
                    createdAt: new Date(cont.Created * 1000).toISOString(),
                    status,
                    uptime: minutes.toString(),
                    ports: cont.Ports.map(
                        p =>
                            `${p.IP ? `${p.IP}:` : ''}${p.PublicPort ? `${p.PublicPort}->` : ''}${p.PrivatePort}/${p.Type}`,
                    ).join(', '),
                    names: cont.Names.map(n => (n.startsWith('/') ? n.substring(1) : n)).join(', '),
                    labels: cont.Labels || {},
                };
            });
        }

        try {
            const { stdout } = await this.#exec(
                `ps ${all ? '-a' : ''} --format  "{{.Names}};{{.Status}};{{.ID}};{{.Image}};{{.Command}};{{.CreatedAt}};{{.Ports}};{{.Labels}}"`,
            );
            return stdout
                .split('\n')
                .filter(line => line.trim() !== '')
                .map(line => {
                    const [names, statusInfo, id, image, command, createdAt, ports, labels] = line.split(';');
                    const [status, ...uptime] = statusInfo.split(' ');
                    let statusKey: ContainerInfo['status'] = status.toLowerCase() as ContainerInfo['status'];
                    if ((statusKey as string) === 'up') {
                        statusKey = 'running';
                    }
                    return {
                        id,
                        image,
                        command,
                        createdAt,
                        status: statusKey,
                        uptime: uptime.join(' '),
                        ports,
                        names,
                        labels:
                            labels?.split(',').reduce(
                                (acc, label) => {
                                    const [key, value] = label.split('=');
                                    if (key && value) {
                                        acc[key] = value;
                                    }
                                    return acc;
                                },
                                {} as { [key: string]: string },
                            ) || {},
                    };
                });
        } catch (e) {
            this.adapter.log.debug(`Cannot list containers: ${e.message}`);
            return [];
        }
    }

    /**
     * Get the logs of a container
     *
     * @param containerNameOrId Container name or ID
     * @param options Options for logs
     * @param options.tail Number of lines to show from the end of the logs
     * @param options.follow If true, follow the logs (not implemented yet)
     */
    async containerLogs(
        containerNameOrId: ContainerName,
        options: { tail?: number; follow?: boolean } = {},
    ): Promise<string[]> {
        if (this.#dockerode) {
            try {
                const container = this.#dockerode.getContainer(containerNameOrId);
                const data = await container.logs({
                    stdout: true,
                    stderr: true,
                    follow: false,
                    tail: options.tail || undefined,
                });
                return data
                    .toString()
                    .split('\n')
                    .filter(line => line.trim() !== '');
            } catch (e) {
                return e
                    .toString()
                    .split('\n')
                    .map((line: string): string => line.trim());
            }
        }

        try {
            const args = [];
            if (options.tail !== undefined) {
                args.push(`--tail ${options.tail}`);
            }
            if (options.follow) {
                args.push(`--follow`);
                throw new Error('Follow option is not implemented yet');
            }
            const result = await this.#exec(`logs${args.length ? ` ${args.join(' ')}` : ''} ${containerNameOrId}`);
            return (result.stdout || result.stderr).split('\n').filter(line => line.trim() !== '');
        } catch (e) {
            return e
                .toString()
                .split('\n')
                .map((line: string): string => line.trim());
        }
    }

    static dockerodeInspect2DockerContainerInspect(data: Docker.ContainerInspectInfo): DockerContainerInspect {
        return {
            ...(data as unknown as DockerContainerInspect),
        };
    }

    /** Inspect a container */
    async containerInspect(containerNameOrId: string): Promise<DockerContainerInspect | null> {
        if (this.#dockerode) {
            try {
                const container = this.#dockerode.getContainer(containerNameOrId);
                const dResult = await container.inspect();

                const result = DockerManager.dockerodeInspect2DockerContainerInspect(dResult);
                if (result.State.Running) {
                    result.Stats = (await this.containerGetRamAndCpuUsage(containerNameOrId)) || undefined;
                }
                return result;
            } catch (e) {
                this.adapter.log.debug(`Cannot inspect container: ${e.message.toString()}`);
                return null;
            }
        }
        try {
            const { stdout } = await this.#exec(`inspect ${containerNameOrId}`);
            const result = JSON.parse(stdout)[0] as DockerContainerInspect;
            if (result.State.Running) {
                result.Stats = (await this.containerGetRamAndCpuUsage(containerNameOrId)) || undefined;
            }
            return result;
        } catch (e) {
            this.adapter.log.debug(`Cannot inspect container: ${e.message.toString()}`);
            return null;
        }
    }

    async containerPrune(): Promise<{ stdout: string; stderr: string }> {
        if (this.#dockerode) {
            try {
                const result = await this.#dockerode.pruneContainers();
                return { stdout: `Containers pruned: ${result.ContainersDeleted?.join(', ') || 'none'}`, stderr: '' };
            } catch (e) {
                return { stdout: '', stderr: e.message.toString() };
            }
        }
        try {
            return await this.#exec(`container prune -f`);
        } catch (e) {
            return { stdout: '', stderr: e.message.toString() };
        }
    }

    /**
     * Build a docker run command string from ContainerConfig
     */
    static toDockerRun(config: ContainerConfig, create?: boolean): string {
        const args: string[] = [];

        // detach / interactive
        if (config.detach !== false && !create) {
            // default is true
            args.push('-d');
        }
        if (config.tty) {
            args.push('-t');
        }
        if (config.stdinOpen) {
            args.push('-i');
        }
        if (config.removeOnExit) {
            args.push('--rm');
        }

        // name
        if (config.name) {
            args.push('--name', config.name);
        }

        // hostname / domain
        if (config.hostname) {
            args.push('--hostname', config.hostname);
        }
        if (config.domainname) {
            args.push('--domainname', config.domainname);
        }

        // environment
        if (config.environment) {
            for (const [key, value] of Object.entries(config.environment)) {
                if (key && value) {
                    args.push('-e', `${key}=${value}`);
                }
            }
        }
        if (config.envFile) {
            for (const file of config.envFile) {
                args.push('--env-file', file);
            }
        }

        // labels
        if (config.labels) {
            for (const [key, value] of Object.entries(config.labels)) {
                args.push('--label', `${key}=${value}`);
            }
        }

        // ports
        if (config.publishAllPorts) {
            args.push('-P');
        }
        if (config.ports) {
            for (const p of config.ports) {
                if (!p.containerPort) {
                    continue;
                }
                const mapping =
                    (p.hostIP ? `${p.hostIP}:` : '') +
                    (p.hostPort ? `${p.hostPort}:` : '') +
                    p.containerPort +
                    (p.protocol ? `/${p.protocol}` : '');
                args.push('-p', mapping);
            }
        }

        // volumes / mounts
        if (config.volumes) {
            for (const v of config.volumes) {
                args.push('-v', v);
            }
        }
        if (config.mounts) {
            for (const m of config.mounts) {
                let mount = `type=${m.type},target=${m.target}`;
                if (m.source) {
                    mount += `,source=${m.source}`;
                }
                if (m.readOnly) {
                    mount += `,readonly`;
                }
                args.push('--mount', mount);
            }
        }

        // restart policy
        if (config.restart?.policy) {
            const val =
                config.restart.policy === 'on-failure' && config.restart.maxRetries
                    ? `on-failure:${config.restart.maxRetries}`
                    : config.restart.policy;
            args.push('--restart', val);
        }

        // user & workdir
        if (config.user) {
            args.push('--user', String(config.user));
        }
        if (config.workdir) {
            args.push('--workdir', config.workdir);
        }

        // logging
        if (config.logging?.driver) {
            args.push('--log-driver', config.logging.driver);
            if (config.logging.options) {
                for (const [k, v] of Object.entries(config.logging.options)) {
                    args.push('--log-opt', `${k}=${v}`);
                }
            }
        }

        // security
        if (config.security?.privileged) {
            args.push('--privileged');
        }
        if (config.security?.capAdd) {
            for (const cap of config.security.capAdd) {
                args.push('--cap-add', cap);
            }
        }
        if (config.security?.capDrop) {
            for (const cap of config.security.capDrop) {
                args.push('--cap-drop', cap);
            }
        }
        if (config.security?.noNewPrivileges) {
            args.push('--security-opt', 'no-new-privileges');
        }
        if (config.security?.apparmor) {
            args.push('--security-opt', `apparmor=${config.security.apparmor}`);
        }

        // network
        if (config.networkMode && typeof config.networkMode === 'string') {
            args.push('--network', config.networkMode);
        }

        // extra hosts
        if (config.extraHosts) {
            for (const host of config.extraHosts as any[]) {
                if (typeof host === 'string') {
                    args.push('--add-host', host);
                } else {
                    args.push('--add-host', `${host.host}:${host.ip}`);
                }
            }
        }

        // sysctls
        if (config.sysctls) {
            for (const [k, v] of Object.entries(config.sysctls)) {
                args.push('--sysctl', `${k}=${v}`);
            }
        }

        // stop signal / timeout
        if (config.stop?.signal) {
            args.push('--stop-signal', config.stop.signal);
        }
        if (config.stop?.gracePeriodSec !== undefined) {
            args.push('--stop-timeout', String(config.stop.gracePeriodSec));
        }

        // resources
        if (config.resources?.cpus) {
            args.push('--cpus', String(config.resources.cpus));
        }
        if (config.resources?.memory) {
            args.push('--memory', String(config.resources.memory));
        }

        // image
        if (!config.image) {
            throw new Error('ContainerConfig.image is required for docker run');
        }
        args.push(config.image);

        // command override
        if (config.command) {
            if (Array.isArray(config.command)) {
                args.push(...config.command);
            } else {
                args.push(config.command);
            }
        }

        return args.join(' ');
    }

    async networkList(): Promise<NetworkInfo[]> {
        if (this.#dockerode) {
            const networks = await this.#dockerode.listNetworks();
            return networks.map(net => ({
                name: net.Name,
                id: net.Id,
                driver: net.Driver as NetworkDriver,
                scope: net.Scope,
            }));
        }
        // docker network ls
        try {
            const { stdout } = await this.#exec(`network ls --format "{{.Name}};{{.ID}};{{.Driver}};{{.Scope}}"`);
            return stdout
                .split('\n')
                .filter(line => line.trim() !== '')
                .map(line => {
                    const [name, id, driver, scope] = line.split(';');
                    return { name, id, driver: driver as NetworkDriver, scope };
                });
        } catch (e) {
            this.adapter.log.debug(`Cannot list networks: ${e.message.toString()}`);
            return [];
        }
    }

    async networkCreate(
        name: string,
        driver?: NetworkDriver,
    ): Promise<{ stdout: string; stderr: string; networks?: NetworkInfo[] }> {
        if (this.#dockerode) {
            const net = await this.#dockerode.createNetwork({ Name: name, Driver: driver || 'bridge' });
            const networks = await this.networkList();
            if (!networks.find(it => it.name === name)) {
                throw new Error(`Network ${name} not found after creation`);
            }
            return { stdout: `Network ${net.id} created`, stderr: '', networks };
        }

        const result = await this.#exec(`network create ${driver ? `--driver ${driver}` : ''} ${name}`);
        const networks = await this.networkList();
        if (!networks.find(it => it.name === name)) {
            throw new Error(`Network ${name} not found after creation`);
        }
        return { ...result, networks };
    }

    async networkRemove(networkId: string): Promise<{ stdout: string; stderr: string; networks?: NetworkInfo[] }> {
        const result = await this.#exec(`network remove ${networkId}`);
        const networks = await this.networkList();
        if (networks.find(it => it.id === networkId)) {
            throw new Error(`Network ${networkId} still found after deletion`);
        }
        return { ...result, networks };
    }

    async networkPrune(): Promise<{ stdout: string; stderr: string; networks?: NetworkInfo[] }> {
        if (this.#dockerode) {
            const result = await this.#dockerode.pruneNetworks();
            const networks = await this.networkList();
            return { stdout: `Networks pruned`, stderr: JSON.stringify(result), networks };
        }
        const result = await this.#exec(`network prune -f`);
        const networks = await this.networkList();
        return { ...result, networks };
    }

    /** List all volumes */
    async volumeList(): Promise<VolumeInfo[]> {
        if (this.#dockerode) {
            const volumesData = await this.#dockerode.listVolumes();
            return (volumesData.Volumes || []).map(vol => ({
                name: vol.Name,
                driver: vol.Driver as VolumeDriver,
                volume: vol.Mountpoint,
            }));
        }
        // docker network ls
        try {
            const { stdout } = await this.#exec(`volume ls --format "{{.Name}};{{.Driver}};{{.Mountpoint}}"`);
            return stdout
                .split('\n')
                .filter(line => line.trim() !== '')
                .map(line => {
                    const [name, driver, volume] = line.split(';');
                    return { name, driver: driver as VolumeDriver, volume };
                });
        } catch (e) {
            this.adapter.log.debug(`Cannot list networks: ${e.message.toString()}`);
            return [];
        }
    }

    async volumeCopyTo(volumeName: string, sourcePath: string): Promise<{ stdout: string; stderr: string }> {
        const tempContainerName = `iobroker_temp_copy_${Date.now()}`;
        if (this.#dockerode) {
            // Check if alpine image is there
            const images = await this.imageList();
            if (!images.find(img => img.repository === 'alpine')) {
                const pullResult = await this.imagePull('alpine');
                if (pullResult.stderr) {
                    return { stdout: '', stderr: `Cannot pull alpine image: ${pullResult.stderr}` };
                }
            }

            // create a temporary container with volume mounted
            const container = await this.#dockerode.createContainer({
                Image: 'alpine',
                name: tempContainerName,
                Cmd: ['sleep', '30'],
                HostConfig: {
                    Binds: [`${volumeName}:/data`],
                },
            });
            try {
                await container.start();
                // Lazy loading tar-fs
                if (!this.#tarPack) {
                    await import('tar-fs')
                        .then(tarFs => (this.#tarPack = tarFs.default.pack))
                        .catch(e => this.adapter.log.error(`Cannot import tar-fs package: ${e.message}`));
                }
                if (!this.#tarPack) {
                    throw new Error('Cannot load tar-fs package');
                }
                // use dockerode to copy files
                const pack = this.#tarPack(sourcePath);
                await container.putArchive(pack, { path: '/data' });
                return { stdout: 'Data copied to volume', stderr: '' };
            } catch (e) {
                return { stdout: '', stderr: `Cannot copy data to volume: ${e.message}` };
            } finally {
                // remove temporary container
                try {
                    await container.stop();
                    await container.remove({ force: true });
                } catch (e) {
                    this.adapter.log.warn(`Cannot remove temporary container ${tempContainerName}: ${e.message}`);
                }
            }
        }

        // create a temporary container with volume mounted
        const createResult = await this.#exec(
            `create -v ${volumeName}:/data --name ${tempContainerName} alpine sleep 60`,
        );
        if (createResult.stderr) {
            return { stdout: '', stderr: `Cannot create temporary container: ${createResult.stderr}` };
        }
        try {
            const copyResult = await this.#exec(`cp ${sourcePath} ${tempContainerName}:/data/`);
            if (copyResult.stderr) {
                return { stdout: '', stderr: `Cannot copy data to volume: ${copyResult.stderr}` };
            }
            return { stdout: 'Data copied to volume', stderr: '' };
        } finally {
            // remove temporary container
            await this.#exec(`rm -f ${tempContainerName}`);
        }
    }

    /**
     * Create a volume
     *
     * @param name Volume name
     * @param driver Volume driver
     * @param volume Volume options (depends on driver)
     */
    async volumeCreate(
        name: string,
        driver?: VolumeDriver,
        volume?: string,
    ): Promise<{ stdout: string; stderr: string; volumes?: VolumeInfo[] }> {
        if (this.#dockerode) {
            const vol = await this.#dockerode.createVolume({
                Name: name,
                Driver: driver || 'local',
                DriverOpts: volume ? { device: volume, o: 'bind', type: 'none' } : {},
            });
            const volumes = await this.volumeList();
            if (!volumes.find(it => it.name === name)) {
                throw new Error(`Network ${name} not found after creation`);
            }
            return { stdout: `Volume ${vol.Name} created`, stderr: '', volumes };
        }
        let result: { stdout: string; stderr: string };
        if (driver === 'local' || !driver) {
            if (volume) {
                result = await this.#exec(
                    `volume create local --opt type=none --opt device=${volume} --opt o=bind ${name}`,
                );
            } else {
                result = await this.#exec(`volume create ${name}`);
            }
        } else {
            throw new Error('not implemented');
        }

        const volumes = await this.volumeList();
        if (!volumes.find(it => it.name === name)) {
            throw new Error(`Network ${name} not found after creation`);
        }
        return { ...result, volumes };
    }

    /** Remove a volume */
    async volumeRemove(volumeName: string): Promise<{ stdout: string; stderr: string; volumes?: VolumeInfo[] }> {
        const result = await this.#exec(`volume remove ${volumeName}`);
        const volumes = await this.volumeList();
        if (volumes.find(it => it.name === volumeName)) {
            throw new Error(`Volume ${volumeName} still found after deletion`);
        }
        return { ...result, volumes };
    }

    /** Prune unused volumes */
    async volumePrune(): Promise<{ stdout: string; stderr: string; volumes?: VolumeInfo[] }> {
        if (this.#dockerode) {
            const result = await this.#dockerode.pruneVolumes();
            const volumes = await this.volumeList();
            return { stdout: `Volumes pruned`, stderr: JSON.stringify(result), volumes };
        }
        const result = await this.#exec(`volume prune -f`);
        const volumes = await this.volumeList();
        return { ...result, volumes };
    }

    /** Stop own containers if necessary */
    async destroy(): Promise<void> {
        if (this.#monitoringInterval) {
            clearInterval(this.#monitoringInterval);
            this.#monitoringInterval = null;
        }

        for (const container of this.#ownContainers) {
            if (container.iobEnabled !== false && container.iobStopOnUnload && container.name) {
                this.adapter.log.info(`Stopping own container ${container.name} on destroy`);
                try {
                    await this.containerStop(container.name);
                } catch (e) {
                    this.adapter.log.warn(`Cannot stop own container ${container.name} on destroy: ${e.message}`);
                }
            }
        }
    }
}
