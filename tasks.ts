import { deleteFoldersRecursive, npmInstall, buildReact, copyFiles } from '@iobroker/build-tools';

const src = `${__dirname}/src-admin/`;

function clean(): void {
    // Only the generated component is deleted here. Everything else in `admin` (jsonConfig.json,
    // jsonCustom.json, the translations and the icon) is source and must survive.
    deleteFoldersRecursive(`${__dirname}/admin/custom`);
    deleteFoldersRecursive(`${src}build`);
}

function copyAllFiles(): void {
    // vite puts the chunks of the component into 'assets', and 'customComponents.js' loads them from
    // there with a relative path - without them the admin cannot start the component
    copyFiles(['src-admin/build/assets/*.js'], 'admin/custom/assets');
    copyFiles(['src-admin/build/assets/*.map'], 'admin/custom/assets');
    copyFiles(['src-admin/build/customComponents.js'], 'admin/custom');
    copyFiles(['src-admin/build/customComponents.js.map'], 'admin/custom');
    // The admin reads this manifest to see which component library the build was made against,
    // and refuses to start the component if it targets an older GUI API generation.
    copyFiles(['src-admin/build/mf-manifest.json'], 'admin/custom');
    copyFiles(['src-admin/src/i18n/*.json'], 'admin/custom/i18n');
}

if (process.argv.includes('--0-clean')) {
    clean();
} else if (process.argv.includes('--1-npm')) {
    npmInstall(src).catch((e: unknown) => console.error(`Cannot install npm: ${e as Error}`));
} else if (process.argv.includes('--2-build')) {
    buildReact(src, { vite: true }).catch((e: unknown) => console.error(`Cannot build: ${e as Error}`));
} else if (process.argv.includes('--3-copy')) {
    copyAllFiles();
} else {
    clean();
    npmInstall(src)
        .then(() => buildReact(src, { vite: true }))
        .then(() => copyAllFiles())
        .catch((e: unknown) => {
            console.error(e);
            process.exit(2);
        });
}
