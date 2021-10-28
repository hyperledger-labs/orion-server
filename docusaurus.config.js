const lightCodeTheme = require('prism-react-renderer/themes/github');
const darkCodeTheme = require('prism-react-renderer/themes/dracula');

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'Hyperledger Orion',
  tagline: 'Documentation',
  url: 'https://hyperledger-labs.github.io',
  baseUrl: '/orion-server/',
  onBrokenLinks: 'throw',
  onBrokenMarkdownLinks: 'warn',
  favicon: 'img/favicon.ico',
  organizationName: 'hyperledger-labs', // Usually your GitHub org/user name.
  projectName: 'orion-server', // Usually your repo name.

  presets: [
    [
      '@docusaurus/preset-classic',
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          sidebarPath: require.resolve('./sidebars.js'),
          // Please change this to your repo.
          // editUrl: 'https://github.com/hyperledger-labs/orion-server/edit/main/docs/',
        },
        blog: {
          showReadingTime: true,
          // Please change this to your repo.
          // editUrl:
            // 'https://github.com/hyperledger-labs/orion-server/edit/main/docs/',
        },
        theme: {
          customCss: require.resolve('./src/css/custom.css'),
        },
      }),
    ],
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      navbar: {
        title: 'Orion',
        logo: {
          alt: 'Orion',
          src: 'img/logo.svg',
        },
        items: [
          {
            to: '/docs/external/introduction',
            label: 'Docs',
            position: 'left',
          },
            {
                to: '/blog',
                label: 'Blog',
                position: 'left'
            },
          {
            href: 'https://github.com/hyperledger-labs/orion-server',
            label: 'GitHub',
            position: 'right',
          },
        ],
      },
      footer: {
        style: 'dark',
        links: [
          {
            title: 'Docs',
            items: [
              {
                label: 'Getting Started',
                to: '/docs/external/getting-started/guide',
              },
            ],
          },
          {
            title: 'Community',
            items: [
              {
                label: 'Stack Overflow',
                href: 'https://stackoverflow.com/questions/tagged/orion',
              },
              {
                label: 'Discord',
                href: 'https://discordapp.com/invite/orion',
              },
              {
                label: 'Twitter',
                href: 'https://twitter.com/orion',
              },
            ],
          },
          {
            title: 'More',
            items: [
              {
                label: 'Blog',
                to: '/blog',
              },
              {
                label: 'GitHub',
                href: 'https://github.com/hyperledger-labs/orion-server',
              },
            ],
          },
        ],
        copyright: `Copyright © ${new Date().getFullYear()} Hyperledger Orion`,
      },
      prism: {
        theme: lightCodeTheme,
        darkTheme: darkCodeTheme,
      },
    }),
};

module.exports = config;
