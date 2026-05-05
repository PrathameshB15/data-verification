// Daily weekly-verification job.
//
// Runs at 08:00 UTC every day. By default verifies the trailing 30 days for
// every active client of every supported CRM (Sticky, Konnektive, VRIO,
// Paysight). All parameters can be overridden when triggering manually.
//
// Prereq on the build node:
//   - python3 + python3-venv (the build creates .venv in the workspace).
//   - config.ini available at the path given by the CONFIG_PATH parameter
//     (default /opt/beastinsights/data-verification/config.ini). It is
//     copied into the workspace before each run so the script sees it.

pipeline {
    agent any

    options {
        timestamps()
        timeout(time: 3, unit: 'HOURS')
        buildDiscarder(logRotator(numToKeepStr: '30'))
    }

    triggers {
        cron('TZ=UTC\n0 8 * * *')
    }

    parameters {
        choice(
            name: 'CRM',
            choices: ['ALL', 'Sticky', 'Konnektive', 'VRIO', 'Paysight'],
            description: 'CRM to verify. ALL runs Sticky, Konnektive, VRIO, Paysight sequentially.'
        )
        string(
            name: 'CLIENT_ID',
            defaultValue: '',
            description: 'Run for a single client_id only. Leave blank to run all active clients of the selected CRM.'
        )
        string(
            name: 'DAYS',
            defaultValue: '30',
            description: 'Trailing days to verify. Ignored when START_DATE and END_DATE are both set.'
        )
        string(
            name: 'START_DATE',
            defaultValue: '',
            description: 'Explicit start date YYYY-MM-DD. Use with END_DATE; overrides DAYS.'
        )
        string(
            name: 'END_DATE',
            defaultValue: '',
            description: 'Explicit end date YYYY-MM-DD. Use with START_DATE; overrides DAYS.'
        )
        string(
            name: 'EXCLUDE_CLIENTS',
            defaultValue: '10000,10027',
            description: 'Comma-separated client IDs to skip (demo accounts by default).'
        )
        string(
            name: 'RETRIES',
            defaultValue: '2',
            description: 'Retries per (client, date) on ERROR.'
        )
        string(
            name: 'MAX_DATE_WORKERS',
            defaultValue: '3',
            description: 'Parallel dates per client.'
        )
        booleanParam(
            name: 'EXPORT',
            defaultValue: false,
            description: 'Write the xlsx report. Off by default; turn on for ad-hoc deep dives.'
        )
        booleanParam(
            name: 'NO_TELEGRAM',
            defaultValue: false,
            description: 'Skip the Telegram notification.'
        )
        string(
            name: 'CONFIG_PATH',
            defaultValue: '/opt/beastinsights/data-verification/config.ini',
            description: 'Where to copy config.ini from (it is gitignored, so it must live outside the repo).'
        )
    }

    stages {
        stage('Setup') {
            steps {
                sh '''
                    set -e
                    if [ ! -f "${CONFIG_PATH}" ]; then
                        echo "config.ini not found at ${CONFIG_PATH}"
                        exit 1
                    fi
                    cp "${CONFIG_PATH}" config.ini
                    chmod 600 config.ini
                    if [ ! -x .venv/bin/python ]; then
                        python3 -m venv .venv
                    fi
                    .venv/bin/pip install --quiet --upgrade pip
                    .venv/bin/pip install --quiet -r requirements.txt
                    .venv/bin/python -c "import index, weekly_verification"
                '''
            }
        }

        stage('Verify') {
            steps {
                script {
                    def crms = params.CRM == 'ALL' ?
                        ['Sticky', 'Konnektive', 'VRIO', 'Paysight'] :
                        [params.CRM]

                    def common = []
                    if (params.CLIENT_ID?.trim()) {
                        common << "--client-id ${params.CLIENT_ID.trim()}"
                    }
                    if (params.START_DATE?.trim() && params.END_DATE?.trim()) {
                        common << "--start-date ${params.START_DATE.trim()}"
                        common << "--end-date ${params.END_DATE.trim()}"
                    } else {
                        common << "--days ${params.DAYS.trim() ?: '30'}"
                    }
                    common << "--retries ${params.RETRIES.trim() ?: '2'}"
                    common << "--max-date-workers ${params.MAX_DATE_WORKERS.trim() ?: '3'}"
                    if (params.EXCLUDE_CLIENTS?.trim()) {
                        common << "--exclude-clients ${params.EXCLUDE_CLIENTS.trim()}"
                    }
                    if (params.EXPORT) {
                        common << "--export"
                    }
                    if (params.NO_TELEGRAM) {
                        common << "--no-telegram"
                    }
                    def commonStr = common.join(' ')

                    def anyFailed = false
                    for (crm in crms) {
                        stage("verify ${crm}") {
                            def cmd = ".venv/bin/python -u weekly_verification.py --crm ${crm} ${commonStr}"
                            echo "Running: ${cmd}"
                            def rc = sh(returnStatus: true, script: cmd)
                            if (rc != 0) {
                                anyFailed = true
                                unstable("CRM ${crm} exited ${rc} — see logs / Telegram for failing dates")
                            }
                        }
                    }
                    if (anyFailed) {
                        currentBuild.result = 'UNSTABLE'
                    }
                }
            }
        }
    }

    post {
        always {
            archiveArtifacts artifacts: '*_weekly_verification_*.xlsx', allowEmptyArchive: true, fingerprint: true
        }
        cleanup {
            sh 'rm -f config.ini'
        }
    }
}

