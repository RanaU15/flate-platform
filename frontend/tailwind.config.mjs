/** @type {import('tailwindcss').Config} */
export default {
  content: ['./src/**/*.{astro,html,js,jsx,md,mdx,svelte,ts,tsx,vue}'],
  theme: {
    extend: {
      colors: {
        apify: {
          orange: '#FF7629',
          dark: '#1B1E2B',
          darker: '#13151F',
          card: '#222634',
          border: '#2F3446',
          text: '#9CA3AF'
        }
      }
    },
  },
  plugins: [],
}

