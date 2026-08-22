using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;

namespace CodexPortableManager
{
    internal sealed class ArtifactPipeline : IDisposable
    {
        private static readonly TimeSpan DefaultDownloadInactivityTimeout = TimeSpan.FromSeconds(30);
        private static readonly TimeSpan DefaultDownloadRetryInitialDelay = TimeSpan.FromSeconds(1);
        private static readonly TimeSpan DefaultDownloadRecoveryWindow = TimeSpan.FromMinutes(30);
        private static readonly TimeSpan MaximumDownloadRetryDelay = TimeSpan.FromSeconds(30);
        private static readonly TimeSpan DownloadProgressReportInterval = TimeSpan.FromMilliseconds(250);
        private static readonly TimeSpan DownloadSpeedWarmupWindow = TimeSpan.FromSeconds(1);
        private static readonly TimeSpan StagingFileAccessRetryInitialDelay = TimeSpan.FromMilliseconds(250);
        private static readonly TimeSpan StagingFileAccessRetryMaximumDelay = TimeSpan.FromSeconds(5);
        private const int MaximumStagingFileAccessRetries = 6;
        private readonly HttpClient httpClient;
        private readonly NetworkAvailabilityMonitor networkMonitor;
        private readonly Action<string> log;
        private readonly TimeSpan downloadInactivityTimeout;
        private readonly TimeSpan downloadRetryInitialDelay;
        private readonly TimeSpan downloadRecoveryWindow;

        public ArtifactPipeline(
            Action<string> logAction,
            Func<string, string, CancellationToken, Task<ProcessResult>> processRunner)
            : this(logAction, processRunner, new HttpClientHandler { AllowAutoRedirect = false })
        {
        }

        internal ArtifactPipeline(
            Action<string> logAction,
            Func<string, string, CancellationToken, Task<ProcessResult>> processRunner,
            HttpMessageHandler httpMessageHandler)
            : this(
                logAction,
                processRunner,
                httpMessageHandler,
                DefaultDownloadInactivityTimeout,
                DefaultDownloadRetryInitialDelay,
                DefaultDownloadRecoveryWindow)
        {
        }

        internal ArtifactPipeline(
            Action<string> logAction,
            Func<string, string, CancellationToken, Task<ProcessResult>> processRunner,
            HttpMessageHandler httpMessageHandler,
            TimeSpan inactivityTimeout)
            : this(
                logAction,
                processRunner,
                httpMessageHandler,
                inactivityTimeout,
                DefaultDownloadRetryInitialDelay,
                DefaultDownloadRecoveryWindow)
        {
        }

        internal ArtifactPipeline(
            Action<string> logAction,
            Func<string, string, CancellationToken, Task<ProcessResult>> processRunner,
            HttpMessageHandler httpMessageHandler,
            TimeSpan inactivityTimeout,
            TimeSpan retryInitialDelay,
            TimeSpan recoveryWindow)
            : this(
                logAction,
                processRunner,
                httpMessageHandler,
                inactivityTimeout,
                retryInitialDelay,
                recoveryWindow,
                null)
        {
        }

        internal ArtifactPipeline(
            Action<string> logAction,
            Func<string, string, CancellationToken, Task<ProcessResult>> processRunner,
            HttpMessageHandler httpMessageHandler,
            TimeSpan inactivityTimeout,
            TimeSpan retryInitialDelay,
            TimeSpan recoveryWindow,
            NetworkAvailabilityMonitor availabilityMonitor)
        {
            log = logAction ?? delegate { };
            if (processRunner == null) throw new ArgumentNullException(nameof(processRunner));
            if (httpMessageHandler == null) throw new ArgumentNullException(nameof(httpMessageHandler));
            if (inactivityTimeout <= TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(inactivityTimeout),
                    "下载停滞超时必须大于零。");
            }
            if (retryInitialDelay <= TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(retryInitialDelay),
                    "下载恢复重试间隔必须大于零。");
            }
            if (recoveryWindow <= TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(recoveryWindow),
                    "下载恢复窗口必须大于零。");
            }
            downloadInactivityTimeout = inactivityTimeout;
            downloadRetryInitialDelay = retryInitialDelay;
            downloadRecoveryWindow = recoveryWindow;
            networkMonitor = availabilityMonitor ?? new NetworkAvailabilityMonitor();
            httpClient = new HttpClient(httpMessageHandler)
            {
                Timeout = System.Threading.Timeout.InfiniteTimeSpan
            };
            httpClient.DefaultRequestHeaders.UserAgent.ParseAdd("CodexPortableManager/1.1.0");
        }

        public async Task<string> DownloadOfficialPackageAsync(
            PackageMetadata package,
            string destinationPath,
            IProgress<OperationProgress> progress,
            OperationPauseToken pauseToken,
            CancellationToken cancellationToken)
        {
            if (progress == null) throw new ArgumentNullException(nameof(progress));
            if (package == null) throw new ArgumentNullException(nameof(package));
            string destination = ValidatePackageDestination(destinationPath);
            string architecture = package.architecture;
            string cacheRoot = PortableStorage.CacheRoot;
            Directory.CreateDirectory(cacheRoot);
            string packagePath = CacheFileLock.GetPackagePath(
                cacheRoot,
                package.packageName,
                package.version,
                architecture);
            string downloadPath = packagePath + ".download-" + Guid.NewGuid().ToString("N") + ".msix";

            try
            {
                string savedPath = await WithVerifiedPackageAsync(
                    package,
                    architecture,
                    packagePath,
                    downloadPath,
                    progress,
                    pauseToken,
                    cancellationToken,
                    async verifiedPath =>
                    {
                        progress.Report(new OperationProgress(
                            "保存官方 MSIX",
                            95,
                            "正在将已验证的微软安装包保存到：" + destination));
                        await CopyVerifiedPackageAsync(
                            verifiedPath,
                            destination,
                            progress,
                            cancellationToken,
                            log).ConfigureAwait(false);
                        return destination;
                    }).ConfigureAwait(false);
                progress.Report(new OperationProgress(
                    "官方 MSIX 下载完成",
                    100,
                    "文件已保存到：" + savedPath + "。程序没有自动安装它。",
                    false,
                    false));
                log("官方 MSIX 已保存到：" + savedPath + "；版本：" + package.version + "。" );
                return savedPath;
            }
            finally
            {
                TryDeleteFile(downloadPath, "官方 MSIX 下载临时文件");
            }
        }

        public async Task<StagingBuildResult> PrepareStagedPackageAsync(
            PackageMetadata package,
            string architecture,
            string packagePath,
            string downloadPath,
            string stagingRoot,
            IProgress<OperationProgress> progress,
            OperationPauseToken pauseToken,
            CancellationToken cancellationToken)
        {
            return await WithVerifiedPackageAsync(
                package,
                architecture,
                packagePath,
                downloadPath,
                progress,
                pauseToken,
                cancellationToken,
                async verifiedPath =>
                {
                    progress.Report(new OperationProgress(
                        "解包并验证官方 MSIX",
                        62,
                        "签名和身份验证通过，正在写入 staging 并同步核对 BlockMap。"));
                    StagingBuildResult build = await ExtractStagingWithFileAccessRetryAsync(
                        verifiedPath,
                        stagingRoot,
                        cancellationToken).ConfigureAwait(false);
                    log(string.Format(
                        CultureInfo.InvariantCulture,
                        "staging 流式构建完成：文件 {0} 个，目录 {1} 个，跳过重复目录探测 {2} 次，工作线程 {3} 个，写入 {4:F1} MiB，验证 BlockMap 块 {5} 个，footprint {6} 个，保留关键摘要 {7} 个/{8:F1} MiB。",
                        build.ExtractedFileCount,
                        build.ValidatedDirectoryCount,
                        build.SkippedDirectoryProbeCount,
                        build.WorkerCount,
                        build.ExtractedBytes / 1048576d,
                        build.VerifiedBlockCount,
                        build.FootprintFileCount,
                        build.OfficialArtifactDigestCount,
                        build.OfficialArtifactDigestBytes / 1048576d));

                    try
                    {
                        progress.Report(new OperationProgress(
                            "确认官方程序结构",
                            74,
                            "BlockMap 已在写入时验证，正在确认 Manifest 身份和关键运行组件。"));
                        build.Profile = ValidateStagedPackage(stagingRoot, package, build.Profile);
                        return build;
                    }
                    catch
                    {
                        build.Dispose();
                        throw;
                    }
                }).ConfigureAwait(false);
        }

        private async Task<T> WithVerifiedPackageAsync<T>(
            PackageMetadata package,
            string architecture,
            string packagePath,
            string downloadPath,
            IProgress<OperationProgress> progress,
            OperationPauseToken pauseToken,
            CancellationToken cancellationToken,
            Func<string, Task<T>> action)
        {
            VerifiedArtifactLease verifiedArtifact = null;
            using (CacheFileLock cacheLock = await CacheFileLock.AcquireAsync(packagePath, cancellationToken).ConfigureAwait(false))
            {
                if (File.Exists(packagePath))
                {
                    FileInfo cachedFile = new FileInfo(packagePath);
                    progress.Report(new OperationProgress(
                        "校验本地安装缓存",
                        8,
                        string.Format(
                            CultureInfo.InvariantCulture,
                            "发现 {0:F1} MiB 缓存，正在通过稳定句柄完成摘要、签名和身份验证。",
                            cachedFile.Length / 1048576d),
                        false,
                        false));
                    try
                    {
                        verifiedArtifact = await VerifyPackageWithProgressAsync(
                            packagePath,
                            package,
                            architecture,
                            progress,
                            cancellationToken).ConfigureAwait(false);
                    }
                    catch (InvalidDataException exception) when (
                        exception.InnerException is MsixPackageDigestMismatchException)
                    {
                        log("缓存大小或 SHA-256 校验失败，将隔离旧文件并重新下载：" + exception.Message);
                    }
                }

                if (verifiedArtifact == null)
                {
                    if (File.Exists(packagePath))
                    {
                        string invalidPath = packagePath + ".invalid-" + Guid.NewGuid().ToString("N");
                        File.Move(packagePath, invalidPath);
                        log("无效缓存已隔离：" + invalidPath);
                    }

                    if (package.localCacheOnly)
                    {
                        throw new InvalidDataException(
                            "缓存回滚包未通过摘要校验，已保留隔离文件，未尝试联网下载。");
                    }

                    PackageAcquisitionResult acquisition = await AcquirePackageBytesAsync(
                        package,
                        Path.GetDirectoryName(packagePath),
                        packagePath,
                        downloadPath,
                        progress,
                        pauseToken,
                        cancellationToken,
                        retainDownloadedHandle: true).ConfigureAwait(false);
                    string actualDigest = acquisition.Sha256Base64;
                    DownloadedPackageLease downloadedPackage = acquisition.DetachDownloadedPackage();

                    progress.Report(new OperationProgress(
                        "校验下载完整性",
                        57,
                        "下载过程中已同步计算 SHA-256，正在核对微软目录摘要并准备发布缓存。",
                        false,
                        null,
                        false,
                        false));
                    if (!string.Equals(actualDigest, package.digest, StringComparison.Ordinal))
                    {
                        if (downloadedPackage != null) downloadedPackage.Dispose();
                        throw new InvalidDataException("下载文件的 SHA-256 与微软元数据不匹配。");
                    }
                    FileStream downloadedStream = downloadedPackage == null
                        ? null
                        : downloadedPackage.DetachStream();
                    if (downloadedPackage != null) downloadedPackage.Dispose();
                    TimeSpan publishElapsed;
                    try
                    {
                        publishElapsed = await PublishDownloadedPackageAsync(
                            downloadPath,
                            packagePath,
                            package.sizeInBytes,
                            progress,
                            cancellationToken).ConfigureAwait(false);
                    }
                    catch
                    {
                        if (downloadedStream != null) downloadedStream.Dispose();
                        throw;
                    }
                    log(string.Format(
                        CultureInfo.InvariantCulture,
                        "程序包获取完成：模式={0}，复用={1:F1} MiB，网络={2:F1} MiB，Range={3}，耗时={4:F1} 秒{5}",
                        acquisition.Mode,
                        acquisition.ReusedBytes / 1048576d,
                        acquisition.RemoteBytes / 1048576d,
                        acquisition.RangeRequestCount,
                        acquisition.Elapsed.TotalSeconds,
                        string.IsNullOrWhiteSpace(acquisition.FallbackReason)
                            ? "。"
                            : "；回退原因：" + acquisition.FallbackReason));
                    log("程序包 SHA-256 校验通过，已原子发布到缓存。" );
                    log(string.Format(
                        CultureInfo.InvariantCulture,
                        "下载缓存发布完成，耗时 {0:F1} 秒。",
                        publishElapsed.TotalSeconds));

                    progress.Report(new OperationProgress(
                        "验证微软商店程序包",
                        null,
                        "正在验证 MSIX 数字签名、Publisher、版本、架构和 PackageFullName。首次读取大型安装包时，Windows 安全扫描可能需要数分钟。",
                        false,
                        false));
                    try
                    {
                        verifiedArtifact = await VerifyPackageWithProgressAsync(
                            packagePath,
                            package,
                            architecture,
                            progress,
                            cancellationToken,
                            downloadedStream,
                            downloadedStream == null ? null : actualDigest).ConfigureAwait(false);
                        downloadedStream = null;
                    }
                    finally
                    {
                        if (downloadedStream != null) downloadedStream.Dispose();
                    }
                }
                else
                {
                    log("程序包获取完成：模式=Cached，网络=0 MiB。");
                    progress.Report(new OperationProgress(
                        "使用已校验的本地缓存",
                        57,
                        "缓存已通过摘要、签名和包身份验证，无需重新下载或重复读取整包。",
                        false,
                        null));
                }
            }

            using (verifiedArtifact)
            {
                cancellationToken.ThrowIfCancellationRequested();
                return await action(verifiedArtifact.PackagePath).ConfigureAwait(false);
            }
        }

        internal async Task<PackageAcquisitionResult> AcquirePackageBytesAsync(
            PackageMetadata package,
            string cacheRoot,
            string packagePath,
            string downloadPath,
            IProgress<OperationProgress> progress,
            OperationPauseToken pauseToken,
            CancellationToken cancellationToken,
            long minimumSavingsBytes = IncrementalAcquisitionPolicy.MinimumSavingsBytes,
            double maximumRemoteFraction = IncrementalAcquisitionPolicy.MaximumRemoteFraction,
            bool retainDownloadedHandle = false)
        {
            if (package == null) throw new ArgumentNullException(nameof(package));
            if (progress == null) throw new ArgumentNullException(nameof(progress));
            if (pauseToken == null) pauseToken = new OperationPauseToken(null);
            Stopwatch stopwatch = Stopwatch.StartNew();
            string fallbackReason = null;
            long incrementalNetworkBytes = 0;
            int incrementalRangeRequests = 0;
            IList<PackageCacheCandidate> candidates = PackageCacheSelector.FindPreviousCandidates(
                cacheRoot,
                package,
                packagePath);
            if (candidates.Count > 0)
            {
                string materializePath = Path.Combine(
                    cacheRoot,
                    ".materialize-" + Guid.NewGuid().ToString("N") + ".msix");
                RemoteRangeReader ranges = null;
                try
                {
                    ranges = new RemoteRangeReader(
                        this,
                        package.url,
                        package.sizeInBytes,
                        pauseToken,
                        progress);
                    MsixZipLayout target = await RemoteMsixLayoutReader.ReadAsync(
                        ranges,
                        package.fullName ?? package.url,
                        cancellationToken).ConfigureAwait(false);
                    List<IncrementalCandidatePlan> viablePlans = new List<IncrementalCandidatePlan>();
                    List<string> candidateFailures = new List<string>();
                    foreach (PackageCacheCandidate candidate in candidates)
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        progress.Report(new OperationProgress(
                            "分析增量更新",
                            10,
                            "正在评估旧缓存 " + candidate.Version + " 与目标版本 " + package.version + "。",
                            true,
                            0));
                        try
                        {
                            MsixZipLayout previous = await Task.Run(
                                () => MsixZipLayout.Read(candidate.Path),
                                cancellationToken).ConfigureAwait(false);
                            PackageReusePlan plan = PackageReusePlanner.CreateForRemoteTarget(previous, target);
                            string policyReason;
                            if (!IncrementalAcquisitionPolicy.ShouldUse(
                                plan,
                                minimumSavingsBytes,
                                maximumRemoteFraction,
                                out policyReason))
                            {
                                candidateFailures.Add(candidate.Version + "：" + policyReason);
                                log("增量候选 " + candidate.Version + " 未达到收益阈值：" + policyReason);
                                continue;
                            }
                            log(string.Format(
                                CultureInfo.InvariantCulture,
                                "增量候选 {0}：复用 {1:F1} MiB，目标补集 {2:F1} MiB，复用条目 {3}/{4}。",
                                candidate.Version,
                                plan.ReusedBytes / 1048576d,
                                plan.TargetBytes / 1048576d,
                                plan.ReusedEntryCount,
                                plan.TargetEntryCount));
                            viablePlans.Add(new IncrementalCandidatePlan(candidate, plan));
                        }
                        catch (OperationCanceledException)
                        {
                            throw;
                        }
                        catch (Exception exception) when (IsIncrementalFallbackException(exception))
                        {
                            string reason = GetExceptionSummary(exception);
                            candidateFailures.Add(candidate.Version + "：" + reason);
                            log("增量候选 " + candidate.Version + " 无法使用，继续评估其他缓存：" + reason);
                        }
                    }

                    viablePlans.Sort(CompareIncrementalCandidates);
                    if (File.Exists(downloadPath))
                    {
                        throw new IOException("增量物化发布路径在操作期间被占用。");
                    }
                    foreach (IncrementalCandidatePlan selected in viablePlans)
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        log(string.Format(
                            CultureInfo.InvariantCulture,
                            "增量计划尝试旧缓存 {0}：已评估 {1} 个候选，预计网络补集 {2:F1} MiB。",
                            selected.Candidate.Version,
                            candidates.Count,
                            selected.Plan.TargetBytes / 1048576d));
                        PackageMaterializationResult materialized;
                        try
                        {
                            materialized = await IncrementalPackageMaterializer.MaterializeFromRemoteTargetAsync(
                                selected.Candidate.Path,
                                materializePath,
                                selected.Plan,
                                ranges,
                                package.digest,
                                progress,
                                cancellationToken).ConfigureAwait(false);
                        }
                        catch (OperationCanceledException)
                        {
                            throw;
                        }
                        catch (Exception exception) when (IsIncrementalFallbackException(exception))
                        {
                            string reason = GetExceptionSummary(exception);
                            candidateFailures.Add(selected.Candidate.Version + "：" + reason);
                            log("增量候选 " + selected.Candidate.Version +
                                " 物化失败，继续尝试下一候选：" + reason);
                            continue;
                        }
                        File.Move(materializePath, downloadPath);
                        log("增量计划最终采用旧缓存 " + selected.Candidate.Version + "。");
                        stopwatch.Stop();
                        return new PackageAcquisitionResult(
                            PackageAcquisitionMode.Incremental,
                            materialized.Sha256Base64,
                            package.sizeInBytes,
                            materialized.ReusedBytes,
                            ranges.NetworkBytesRead,
                            ranges.RequestCount,
                            null,
                            stopwatch.Elapsed);
                    }
                    fallbackReason = candidateFailures.Count == 0
                        ? "所有旧缓存都无法生成可用的增量计划。"
                        : "所有旧缓存均不可用：" + string.Join("；", candidateFailures.ToArray());
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
                catch (Exception exception) when (IsIncrementalFallbackException(exception))
                {
                    fallbackReason = GetExceptionSummary(exception);
                }
                finally
                {
                    if (ranges != null)
                    {
                        incrementalNetworkBytes = ranges.NetworkBytesRead;
                        incrementalRangeRequests = ranges.RequestCount;
                    }
                    TryDeleteFile(materializePath, "增量物化临时文件");
                }
            }
            else
            {
                fallbackReason = "没有找到同架构且低于目标版本的正式 MSIX 缓存。";
            }

            log("增量更新未采用，将回退完整下载：" + fallbackReason);
            progress.Report(new OperationProgress(
                "下载微软官方程序包",
                10,
                "正在回退完整下载，目标约 " +
                (package.sizeInBytes / 1048576d).ToString("F1", CultureInfo.InvariantCulture) +
                " MiB。",
                true,
                0));
            TryDeleteFile(downloadPath, "完整下载目标临时文件");
            DownloadedPackageLease downloadedPackage = await DownloadFileWithLeaseAsync(
                package.url,
                downloadPath,
                package.sizeInBytes,
                progress,
                pauseToken,
                cancellationToken).ConfigureAwait(false);
            stopwatch.Stop();
            string digest = downloadedPackage.Sha256Base64;
            DownloadedPackageLease retainedPackage = retainDownloadedHandle ? downloadedPackage : null;
            if (!retainDownloadedHandle) downloadedPackage.Dispose();
            return new PackageAcquisitionResult(
                PackageAcquisitionMode.FullDownload,
                digest,
                package.sizeInBytes,
                0,
                checked(package.sizeInBytes + incrementalNetworkBytes),
                incrementalRangeRequests,
                fallbackReason,
                stopwatch.Elapsed,
                retainedPackage);
        }

        private static int CompareIncrementalCandidates(
            IncrementalCandidatePlan first,
            IncrementalCandidatePlan second)
        {
            int comparison = first.Plan.TargetBytes.CompareTo(second.Plan.TargetBytes);
            if (comparison != 0) return comparison;
            comparison = second.Plan.ReusedBytes.CompareTo(first.Plan.ReusedBytes);
            if (comparison != 0) return comparison;
            return second.Candidate.Version.CompareTo(first.Candidate.Version);
        }

        private static bool IsIncrementalFallbackException(Exception exception)
        {
            return exception is InvalidDataException ||
                exception is IOException ||
                exception is HttpRequestException ||
                exception is UnauthorizedAccessException ||
                exception is InvalidOperationException ||
                exception is ArgumentException ||
                exception is OverflowException ||
                exception is NotSupportedException;
        }

        private async Task<VerifiedArtifactLease> VerifyPackageWithProgressAsync(
            string packagePath,
            PackageMetadata package,
            string architecture,
            IProgress<OperationProgress> progress,
            CancellationToken cancellationToken,
            FileStream lockedStream = null,
            string trustedSha256Base64 = null)
        {
            using (CancellationTokenSource heartbeatCancellation = new CancellationTokenSource())
            {
                Stopwatch stopwatch = Stopwatch.StartNew();
                Task heartbeat = ReportVerificationHeartbeatAsync(
                    package.sizeInBytes,
                    stopwatch,
                    progress,
                    heartbeatCancellation.Token);
                try
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    return await Task.Run(
                        () => MsixPackageTrust.VerifyAndLock(
                            packagePath,
                            package,
                            architecture,
                            log,
                            lockedStream,
                            trustedSha256Base64)).ConfigureAwait(false);
                }
                finally
                {
                    stopwatch.Stop();
                    heartbeatCancellation.Cancel();
                    try { await heartbeat.ConfigureAwait(false); }
                    catch (OperationCanceledException) { }
                }
            }
        }

        private async Task<StagingBuildResult> ExtractStagingWithFileAccessRetryAsync(
            string packagePath,
            string stagingRoot,
            CancellationToken cancellationToken)
        {
            for (int attempt = 0; ; attempt++)
            {
                try
                {
                    return await StagingBuilder.ExtractAndValidateAsync(
                        packagePath,
                        stagingRoot,
                        cancellationToken).ConfigureAwait(false);
                }
                catch (IOException exception) when (
                    IsTransientFileSharingException(exception) &&
                    attempt < MaximumStagingFileAccessRetries)
                {
                    TimeSpan delay = GetStagingFileAccessRetryDelay(attempt);
                    string delayText = delay < TimeSpan.FromSeconds(1)
                        ? Math.Ceiling(delay.TotalMilliseconds).ToString("F0", CultureInfo.InvariantCulture) + " 毫秒"
                        : FormatDuration(delay);
                    log(string.Format(
                        CultureInfo.InvariantCulture,
                        "MSIX 解包暂时无法读取缓存文件（HResult=0x{0:X8}），将在 {1} 后重试第 {2} 次。",
                        unchecked((uint)exception.HResult),
                        delayText,
                        attempt + 1));
                    await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
                }
            }
        }

        private static TimeSpan GetStagingFileAccessRetryDelay(int attempt)
        {
            int exponent = Math.Max(0, Math.Min(5, attempt));
            double milliseconds = StagingFileAccessRetryInitialDelay.TotalMilliseconds * (1 << exponent);
            return TimeSpan.FromMilliseconds(Math.Min(
                StagingFileAccessRetryMaximumDelay.TotalMilliseconds,
                milliseconds));
        }

        private static bool IsTransientFileSharingException(Exception exception)
        {
            Exception current = exception;
            while (current != null)
            {
                IOException io = current as IOException;
                if (io != null)
                {
                    int hresult = io.HResult;
                    if (hresult == unchecked((int)0x80070020) ||
                        hresult == unchecked((int)0x80070021))
                    {
                        return true;
                    }
                }
                current = current.InnerException;
            }
            return false;
        }

        private static async Task ReportVerificationHeartbeatAsync(
            long packageSize,
            Stopwatch stopwatch,
            IProgress<OperationProgress> progress,
            CancellationToken cancellationToken)
        {
            while (true)
            {
                await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken).ConfigureAwait(false);
                progress.Report(new OperationProgress(
                    "等待 Windows 完成安装包检查",
                    null,
                    string.Format(
                        CultureInfo.InvariantCulture,
                        "正在扫描并验证约 {0:F1} MiB 的 MSIX，已等待 {1}。首次检查可能耗时数分钟，请勿重复启动下载。",
                        packageSize / 1048576d,
                        FormatDuration(stopwatch.Elapsed)),
                    false,
                    false));
            }
        }

        private static async Task<TimeSpan> PublishDownloadedPackageAsync(
            string downloadPath,
            string packagePath,
            long packageSize,
            IProgress<OperationProgress> progress,
            CancellationToken cancellationToken)
        {
            using (CancellationTokenSource heartbeatCancellation = new CancellationTokenSource())
            {
                Stopwatch stopwatch = Stopwatch.StartNew();
                Task heartbeat = ReportCachePublishHeartbeatAsync(
                    packageSize,
                    stopwatch,
                    progress,
                    heartbeatCancellation.Token);
                try
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    await Task.Run(
                        () => File.Move(downloadPath, packagePath)).ConfigureAwait(false);
                    cancellationToken.ThrowIfCancellationRequested();
                    return stopwatch.Elapsed;
                }
                finally
                {
                    stopwatch.Stop();
                    heartbeatCancellation.Cancel();
                    try { await heartbeat.ConfigureAwait(false); }
                    catch (OperationCanceledException) { }
                }
            }
        }

        private static async Task ReportCachePublishHeartbeatAsync(
            long packageSize,
            Stopwatch stopwatch,
            IProgress<OperationProgress> progress,
            CancellationToken cancellationToken)
        {
            while (true)
            {
                await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken).ConfigureAwait(false);
                progress.Report(new OperationProgress(
                    "等待系统完成缓存发布",
                    null,
                    string.Format(
                        CultureInfo.InvariantCulture,
                        "约 {0:F1} MiB 的 MSIX 已下载并通过摘要校验；Windows 安全扫描正在占用文件，已等待 {1}。",
                        packageSize / 1048576d,
                        FormatDuration(stopwatch.Elapsed)),
                    false,
                    false));
            }
        }

        internal static async Task CopyVerifiedPackageAsync(
            string sourcePath,
            string destinationPath,
            IProgress<OperationProgress> progress,
            CancellationToken cancellationToken,
            Action<string> logAction = null)
        {
            if (progress == null) throw new ArgumentNullException(nameof(progress));
            cancellationToken.ThrowIfCancellationRequested();
            string source = Path.GetFullPath(sourcePath);
            string destination = ValidatePackageDestination(destinationPath);
            if (string.Equals(source, destination, StringComparison.OrdinalIgnoreCase))
            {
                return;
            }

            string parent = Path.GetDirectoryName(destination);
            if (string.IsNullOrWhiteSpace(parent))
            {
                throw new InvalidDataException("官方 MSIX 保存目录无效。");
            }
            Directory.CreateDirectory(parent);
            string temporary = destination + ".download-" + Guid.NewGuid().ToString("N") + ".msix";
            string temporaryIdentity = null;
            try
            {
                byte[] buffer = new byte[1024 * 1024];
                using (FileStream input = new FileStream(
                    source,
                    FileMode.Open,
                    FileAccess.Read,
                    FileShare.Read | FileShare.Delete,
                    buffer.Length,
                    FileOptions.Asynchronous | FileOptions.SequentialScan))
                using (FileStream output = new FileStream(
                    temporary,
                    FileMode.CreateNew,
                    FileAccess.Write,
                    FileShare.None,
                    buffer.Length,
                    FileOptions.Asynchronous | FileOptions.SequentialScan))
                {
                    temporaryIdentity = NativeFileSystem.GetPersistentFileIdentity(
                        output.SafeFileHandle,
                        temporary);
                    long copied = 0;
                    int read;
                    while ((read = await input.ReadAsync(
                        buffer,
                        0,
                        buffer.Length,
                        cancellationToken).ConfigureAwait(false)) > 0)
                    {
                        await output.WriteAsync(
                            buffer,
                            0,
                            read,
                            cancellationToken).ConfigureAwait(false);
                        copied += read;
                        progress.Report(new OperationProgress(
                            "保存官方 MSIX",
                            95,
                            string.Format(
                                CultureInfo.InvariantCulture,
                                "正在复制已验证的微软安装包：{0:F1} / {1:F1} MiB。",
                                copied / 1048576d,
                                input.Length / 1048576d)));
                        cancellationToken.ThrowIfCancellationRequested();
                    }
                    cancellationToken.ThrowIfCancellationRequested();
                    progress.Report(new OperationProgress(
                        "提交官方 MSIX",
                        99,
                        "复制已完成，正在刷新文件并原子替换保存目标。",
                        false,
                        false));
                    cancellationToken.ThrowIfCancellationRequested();
                    output.Flush(true);
                }
                if (File.Exists(destination))
                {
                    File.Replace(temporary, destination, null, true);
                }
                else
                {
                    File.Move(temporary, destination);
                }
            }
            finally
            {
                await CleanupSavedPackageTemporaryAsync(
                    temporary,
                    temporaryIdentity,
                    progress,
                    logAction).ConfigureAwait(false);
            }
        }

        private static async Task CleanupSavedPackageTemporaryAsync(
            string temporaryPath,
            string temporaryIdentity,
            IProgress<OperationProgress> progress,
            Action<string> logAction)
        {
            Exception lastError = null;
            for (int attempt = 0; attempt < 4; attempt++)
            {
                try
                {
                    NativeFileSystem.DeleteFile(temporaryPath, temporaryIdentity);
                    return;
                }
                catch (Exception exception)
                {
                    lastError = exception;
                    if (attempt < 3)
                    {
                        await Task.Delay(TimeSpan.FromMilliseconds(100 * (attempt + 1)))
                            .ConfigureAwait(false);
                    }
                }
            }

            string warning = "警告：无法清理官方 MSIX 保存临时文件：" +
                temporaryPath + "。" +
                (lastError == null ? "请手动删除该文件。" : lastError.Message);
            if (logAction != null) logAction(warning);
            progress.Report(new OperationProgress(
                "临时文件清理未完成",
                null,
                warning,
                false,
                false));
        }

        private static string ValidatePackageDestination(string path)
        {
            if (string.IsNullOrWhiteSpace(path))
            {
                throw new ArgumentException("官方 MSIX 保存路径不能为空。", nameof(path));
            }
            string fullPath = Path.GetFullPath(Environment.ExpandEnvironmentVariables(path));
            if (!string.Equals(Path.GetExtension(fullPath), ".msix", StringComparison.OrdinalIgnoreCase))
            {
                throw new ArgumentException("官方 MSIX 保存路径必须以 .msix 结尾。", nameof(path));
            }
            if (Directory.Exists(fullPath))
            {
                throw new ArgumentException("官方 MSIX 保存路径不能是目录。", nameof(path));
            }
            return fullPath;
        }

        internal async Task<string> DownloadFileAsync(
            string url,
            string destination,
            long expectedSize,
            IProgress<OperationProgress> progress,
            OperationPauseToken pauseToken,
            CancellationToken cancellationToken)
        {
            using (DownloadedPackageLease downloadedPackage = await DownloadFileWithLeaseAsync(
                url,
                destination,
                expectedSize,
                progress,
                pauseToken,
                cancellationToken).ConfigureAwait(false))
            {
                return downloadedPackage.Sha256Base64;
            }
        }

        private async Task<DownloadedPackageLease> DownloadFileWithLeaseAsync(
            string url,
            string destination,
            long expectedSize,
            IProgress<OperationProgress> progress,
            OperationPauseToken pauseToken,
            CancellationToken cancellationToken)
        {
            if (progress == null) throw new ArgumentNullException(nameof(progress));
            if (pauseToken == null) pauseToken = new OperationPauseToken(null);
            Exception lastException = null;
            int consecutiveFailures = 0;
            int totalFailures = 0;
            Stopwatch noProgressStopwatch = Stopwatch.StartNew();
            int resumeVersion = pauseToken.ResumeVersion;

            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await pauseToken.WaitWhilePausedAsync(cancellationToken).ConfigureAwait(false);
                if (pauseToken.ResumeVersion != resumeVersion)
                {
                    resumeVersion = pauseToken.ResumeVersion;
                    noProgressStopwatch.Restart();
                }
                if (lastException != null && noProgressStopwatch.Elapsed >= downloadRecoveryWindow)
                {
                    break;
                }
                long lengthBeforeAttempt = GetDownloadLength(destination);
                try
                {
                    DownloadedPackageLease result = await DownloadFileFromUrlWithLeaseAsync(
                        url,
                        destination,
                        expectedSize,
                        progress,
                        pauseToken,
                        cancellationToken).ConfigureAwait(false);
                    if (totalFailures > 0)
                    {
                        log("网络已恢复，下载已自动继续。");
                    }
                    return result;
                }
                catch (DownloadPausedException)
                {
                    await pauseToken.WaitWhilePausedAsync(cancellationToken).ConfigureAwait(false);
                    noProgressStopwatch.Restart();
                }
                catch (DownloadRetryRequestedException)
                {
                    log("已收到立即重试请求，正在重新连接微软 CDN。");
                    noProgressStopwatch.Restart();
                }
                catch (Exception exception) when (IsRetryableDownloadException(exception, cancellationToken))
                {
                    lastException = exception;
                    long preservedLength = GetDownloadLength(destination);
                    if (preservedLength > lengthBeforeAttempt)
                    {
                        consecutiveFailures = 0;
                        noProgressStopwatch.Restart();
                    }
                    if (pauseToken.ResumeVersion != resumeVersion)
                    {
                        resumeVersion = pauseToken.ResumeVersion;
                        noProgressStopwatch.Restart();
                    }
                    consecutiveFailures++;
                    totalFailures++;
                    TransientHttpRequestException transientStatus = exception as TransientHttpRequestException;
                    TimeSpan retryDelay = GetDownloadRetryDelay(
                        consecutiveFailures,
                        transientStatus == null ? null : transientStatus.RetryAfter);
                    TimeSpan remainingRecoveryWindow = downloadRecoveryWindow - noProgressStopwatch.Elapsed;
                    if (remainingRecoveryWindow <= TimeSpan.Zero)
                    {
                        break;
                    }
                    if (retryDelay > remainingRecoveryWindow)
                    {
                        retryDelay = remainingRecoveryWindow;
                    }
                    int operationPercent = GetOperationDownloadPercent(preservedLength, expectedSize);
                    int downloadPercent = GetFileDownloadPercent(preservedLength, expectedSize);
                    bool internetAvailable = networkMonitor.HasInternetAccess;
                    progress.Report(new OperationProgress(
                        internetAvailable
                            ? "微软 CDN 暂不可达，已自动暂停"
                            : "网络不可用，已自动暂停",
                        operationPercent,
                        string.Format(
                            CultureInfo.InvariantCulture,
                            internetAvailable
                                ? "已自动保留 {0:F1} MiB，{1}后进行第 {2} 次探测；网络变化会立即唤醒。"
                                : "已自动保留 {0:F1} MiB，正在监听系统网络恢复；恢复后立即进行第 {2} 次探测。",
                            preservedLength / 1048576d,
                            FormatDuration(retryDelay),
                            totalFailures),
                        true,
                        downloadPercent,
                        true));
                    log(string.Format(
                        CultureInfo.InvariantCulture,
                        internetAvailable
                            ? "下载连接中断，已自动暂停；已保留 {0:F1} MiB，将在 {1}后从断点探测：{2}"
                            : "系统网络不可用，已自动暂停；已保留 {0:F1} MiB，网络恢复后将立即从断点探测：{2}",
                        preservedLength / 1048576d,
                        FormatDuration(retryDelay),
                        GetExceptionSummary(exception)));
                    await WaitForDownloadRetryAsync(retryDelay, pauseToken, cancellationToken).ConfigureAwait(false);
                }
            }
            throw new InvalidOperationException(
                "无法下载微软官方程序包：" + GetExceptionSummary(lastException) +
                "。程序已在 " + FormatDuration(downloadRecoveryWindow) +
                " 的恢复窗口内保留断点并持续重试；请检查系统代理、VPN 或网络分流后重新操作。",
                lastException);
        }

        internal async Task<string> DownloadFileFromUrlAsync(
            string url,
            string destination,
            long expectedSize,
            IProgress<OperationProgress> progress,
            CancellationToken cancellationToken)
        {
            return await DownloadFileFromUrlAsync(
                url,
                destination,
                expectedSize,
                progress,
                new OperationPauseToken(null),
                cancellationToken).ConfigureAwait(false);
        }

        internal async Task<string> DownloadFileFromUrlAsync(
            string url,
            string destination,
            long expectedSize,
            IProgress<OperationProgress> progress,
            OperationPauseToken pauseToken,
            CancellationToken cancellationToken)
        {
            using (DownloadedPackageLease downloadedPackage = await DownloadFileFromUrlWithLeaseAsync(
                url,
                destination,
                expectedSize,
                progress,
                pauseToken,
                cancellationToken).ConfigureAwait(false))
            {
                return downloadedPackage.Sha256Base64;
            }
        }

        private async Task<DownloadedPackageLease> DownloadFileFromUrlWithLeaseAsync(
            string url,
            string destination,
            long expectedSize,
            IProgress<OperationProgress> progress,
            OperationPauseToken pauseToken,
            CancellationToken cancellationToken)
        {
            if (progress == null) throw new ArgumentNullException(nameof(progress));
            if (pauseToken == null) pauseToken = new OperationPauseToken(null);
            await pauseToken.WaitWhilePausedAsync(cancellationToken).ConfigureAwait(false);
            long requestedOffset = GetResumeOffset(destination, expectedSize);
            if (expectedSize > 0 && requestedOffset == expectedSize)
            {
                progress.Report(new OperationProgress(
                    "校验已完成的下载断点",
                    55,
                    "临时文件已达到目录声明的完整大小，正在计算 SHA-256。",
                    false,
                    null));
                return await Task.Run(
                    () => OpenCompletedDownload(destination)).ConfigureAwait(false);
            }

            using (HttpResponseMessage response = await SendDownloadRequestAsync(
                url,
                requestedOffset,
                pauseToken,
                cancellationToken).ConfigureAwait(false))
            {
                if (HttpRetryPolicy.IsTransientStatus(response.StatusCode))
                {
                    throw new TransientHttpRequestException(
                        "微软 CDN 返回可重试的 HTTP 状态：" + (int)response.StatusCode + "。",
                        response.StatusCode,
                        HttpRetryPolicy.GetRetryAfter(response.Headers));
                }
                long appendOffset = 0;
                if (response.StatusCode == HttpStatusCode.PartialContent)
                {
                    ValidatePartialContentResponse(response, requestedOffset, expectedSize);
                    appendOffset = requestedOffset;
                }
                else if (requestedOffset > 0 && response.StatusCode == HttpStatusCode.OK)
                {
                    log("微软 CDN 未接受 Range 续传请求，将安全地从头重新下载当前文件。");
                }
                if (!response.IsSuccessStatusCode)
                {
                    throw new InvalidDataException(
                        "微软 CDN 返回不可重试的 HTTP 状态：" +
                        (int)response.StatusCode + " " + response.ReasonPhrase + "。");
                }
                long? contentLength = response.Content.Headers.ContentLength;
                long expectedResponseLength = expectedSize > 0 ? expectedSize - appendOffset : -1;
                if (expectedResponseLength >= 0 &&
                    contentLength.HasValue &&
                    contentLength.Value != expectedResponseLength)
                {
                    throw new InvalidDataException(string.Format(
                        CultureInfo.InvariantCulture,
                        "微软 CDN Content-Length 与续传范围不一致，预期 {0} 字节，实际 {1} 字节。",
                        expectedResponseLength,
                        contentLength.Value));
                }
                long total = expectedSize > 0
                    ? expectedSize
                    : appendOffset + (contentLength ?? 0);
                long downloaded = appendOffset;
                using (Stream input = await response.Content.ReadAsStreamAsync().ConfigureAwait(false))
                {
                    FileStream output = new FileStream(
                    destination,
                    FileMode.OpenOrCreate,
                    FileAccess.ReadWrite,
                    FileShare.Read | FileShare.Delete,
                    1024 * 1024,
                    FileOptions.Asynchronous | FileOptions.SequentialScan);
                    try
                    {
                        using (SHA256 sha256 = SHA256.Create())
                        {
                    byte[] buffer = new byte[1024 * 1024];
                    if (appendOffset == 0)
                    {
                        output.SetLength(0);
                    }
                    else
                    {
                        if (output.Length != appendOffset)
                        {
                            throw new IOException("下载断点文件在续传准备期间发生了变化。");
                        }
                        progress.Report(new OperationProgress(
                            "准备从断点继续下载",
                            GetOperationDownloadPercent(appendOffset, expectedSize),
                            string.Format(
                                CultureInfo.InvariantCulture,
                                "正在校验已保留的 {0:F1} MiB 下载片段。",
                                appendOffset / 1048576d),
                            true,
                            GetFileDownloadPercent(appendOffset, expectedSize)));
                        await HashExistingPrefixAsync(
                            output,
                            sha256,
                            appendOffset,
                            buffer,
                            pauseToken,
                            cancellationToken).ConfigureAwait(false);
                    }
                    output.Position = appendOffset;
                    int lastPercent = -1;
                    Stopwatch stopwatch = Stopwatch.StartNew();
                    TimeSpan lastProgressReportElapsed = TimeSpan.Zero;
                    long receivedThisAttempt = 0;
                    int read;
                    while (true)
                    {
                        read = await ReadDownloadChunkAsync(
                            input,
                            buffer,
                            pauseToken,
                            cancellationToken).ConfigureAwait(false);
                        if (read <= 0) break;
                        await output.WriteAsync(buffer, 0, read, cancellationToken).ConfigureAwait(false);
                        sha256.TransformBlock(buffer, 0, read, null, 0);
                        downloaded += read;
                        receivedThisAttempt += read;
                        if (expectedSize > 0 && downloaded > expectedSize)
                        {
                            throw new InvalidDataException("微软 CDN 返回的数据超过目录声明的程序包大小。");
                        }
                        int percent = total > 0 ? (int)Math.Min(99, downloaded * 100L / total) : 0;
                        TimeSpan elapsed = stopwatch.Elapsed;
                        if (percent != lastPercent ||
                            elapsed - lastProgressReportElapsed >= DownloadProgressReportInterval)
                        {
                            lastPercent = percent;
                            lastProgressReportElapsed = elapsed;
                            bool speedReady = elapsed >= DownloadSpeedWarmupWindow;
                            double seconds = Math.Max(0.001, elapsed.TotalSeconds);
                            double speed = speedReady ? receivedThisAttempt / 1048576d / seconds : 0;
                            double remainingSeconds = speed > 0 && total > downloaded
                                ? (total - downloaded) / 1048576d / speed
                                : 0;
                            string speedText = speedReady
                                ? speed.ToString("F1", CultureInfo.InvariantCulture) + " MiB/s"
                                : "测速中（MiB/s）";
                            progress.Report(new OperationProgress(
                                "下载微软官方程序包",
                                10 + (int)(percent * 45L / 100L),
                                string.Format(
                                    CultureInfo.InvariantCulture,
                                    "{0:F1} / {1:F1} MiB · {2}{3}",
                                    downloaded / 1048576d,
                                    total / 1048576d,
                                    speedText,
                                    remainingSeconds > 1 ? " · 预计剩余 " + FormatDuration(TimeSpan.FromSeconds(remainingSeconds)) : string.Empty),
                                true,
                                percent));
                        }
                    }
                    sha256.TransformFinalBlock(new byte[0], 0, 0);
                    await output.FlushAsync(cancellationToken).ConfigureAwait(false);
                    if (expectedSize > 0 && downloaded != expectedSize)
                    {
                        throw new DownloadTransportException(string.Format(
                            CultureInfo.InvariantCulture,
                            "微软 CDN 响应提前结束，已保留 {0} 字节，预期 {1} 字节。",
                            downloaded,
                            expectedSize));
                    }
                            DownloadedPackageLease result = new DownloadedPackageLease(
                                Convert.ToBase64String(sha256.Hash),
                                output);
                            output = null;
                            return result;
                        }
                    }
                    finally
                    {
                        if (output != null) output.Dispose();
                    }
                }
            }
        }

        private static DownloadedPackageLease OpenCompletedDownload(string path)
        {
            FileStream stream = new FileStream(
                path,
                FileMode.Open,
                FileAccess.ReadWrite,
                FileShare.Read | FileShare.Delete,
                1024 * 1024,
                FileOptions.SequentialScan);
            try
            {
                using (SHA256 sha = SHA256.Create())
                {
                    string digest = Convert.ToBase64String(sha.ComputeHash(stream));
                    stream.Position = 0;
                    DownloadedPackageLease result = new DownloadedPackageLease(digest, stream);
                    stream = null;
                    return result;
                }
            }
            finally
            {
                if (stream != null) stream.Dispose();
            }
        }

        private static async Task HashExistingPrefixAsync(
            FileStream output,
            SHA256 sha256,
            long prefixLength,
            byte[] buffer,
            OperationPauseToken pauseToken,
            CancellationToken cancellationToken)
        {
            output.Position = 0;
            long remaining = prefixLength;
            while (remaining > 0)
            {
                await pauseToken.WaitWhilePausedAsync(cancellationToken).ConfigureAwait(false);
                int requested = (int)Math.Min(buffer.Length, remaining);
                int read = await output.ReadAsync(
                    buffer,
                    0,
                    requested,
                    cancellationToken).ConfigureAwait(false);
                if (read <= 0)
                {
                    throw new IOException("无法读取完整的下载断点文件。");
                }
                sha256.TransformBlock(buffer, 0, read, null, 0);
                remaining -= read;
            }
        }

        private static void ValidatePartialContentResponse(
            HttpResponseMessage response,
            long requestedOffset,
            long expectedSize)
        {
            ContentRangeHeaderValue range = response.Content.Headers.ContentRange;
            if (range == null ||
                !string.Equals(range.Unit, "bytes", StringComparison.OrdinalIgnoreCase) ||
                !range.From.HasValue ||
                range.From.Value != requestedOffset ||
                !range.To.HasValue ||
                range.To.Value < range.From.Value)
            {
                throw new InvalidDataException("微软 CDN 返回了无效的 Content-Range，无法安全续传。");
            }
            if (expectedSize > 0 &&
                (!range.Length.HasValue ||
                 range.Length.Value != expectedSize ||
                 range.To.Value != expectedSize - 1))
            {
                throw new InvalidDataException("微软 CDN 返回的 Content-Range 与目录声明大小不一致。");
            }
        }

        private static long GetDownloadLength(string destination)
        {
            try
            {
                return File.Exists(destination) ? new FileInfo(destination).Length : 0;
            }
            catch
            {
                return 0;
            }
        }

        private static long GetResumeOffset(string destination, long expectedSize)
        {
            long length = GetDownloadLength(destination);
            if (length <= 0 || expectedSize <= 0 || length > expectedSize)
            {
                return 0;
            }
            return length;
        }

        internal TimeSpan GetDownloadRetryDelay(int consecutiveFailures, TimeSpan? serverDelay)
        {
            if (serverDelay.HasValue)
            {
                return serverDelay.Value > MaximumDownloadRetryDelay
                    ? MaximumDownloadRetryDelay
                    : serverDelay.Value;
            }
            if (downloadRetryInitialDelay <= TimeSpan.Zero)
            {
                return TimeSpan.Zero;
            }
            int exponent = Math.Max(0, Math.Min(5, consecutiveFailures - 1));
            double milliseconds = downloadRetryInitialDelay.TotalMilliseconds * (1 << exponent);
            return TimeSpan.FromMilliseconds(Math.Min(
                MaximumDownloadRetryDelay.TotalMilliseconds,
                milliseconds));
        }

        private static int GetOperationDownloadPercent(long downloaded, long expectedSize)
        {
            if (expectedSize <= 0) return 10;
            int percent = (int)Math.Min(100, Math.Max(0, downloaded) * 100L / expectedSize);
            return 10 + (int)(percent * 45L / 100L);
        }

        private static int GetFileDownloadPercent(long downloaded, long expectedSize)
        {
            if (expectedSize <= 0) return 0;
            return (int)Math.Min(100, Math.Max(0, downloaded) * 100L / expectedSize);
        }

        internal async Task<HttpResponseMessage> SendDownloadRequestAsync(
            string url,
            CancellationToken cancellationToken)
        {
            return await SendDownloadRequestAsync(
                url,
                0,
                new OperationPauseToken(null),
                cancellationToken).ConfigureAwait(false);
        }

        internal async Task<HttpResponseMessage> SendDownloadRequestAsync(
            string url,
            long resumeOffset,
            CancellationToken cancellationToken)
        {
            return await SendDownloadRequestAsync(
                url,
                resumeOffset,
                new OperationPauseToken(null),
                cancellationToken).ConfigureAwait(false);
        }

        internal async Task<HttpResponseMessage> SendDownloadRequestAsync(
            string url,
            long resumeOffset,
            OperationPauseToken pauseToken,
            CancellationToken cancellationToken)
        {
            if (resumeOffset < 0) throw new ArgumentOutOfRangeException(nameof(resumeOffset));
            return await SendPackageRequestAsync(
                url,
                resumeOffset > 0 ? (long?)resumeOffset : null,
                null,
                pauseToken,
                cancellationToken).ConfigureAwait(false);
        }

        internal async Task<HttpResponseMessage> SendRangeRequestAsync(
            string url,
            long start,
            long end,
            CancellationToken cancellationToken)
        {
            return await SendRangeRequestAsync(
                url,
                start,
                end,
                new OperationPauseToken(null),
                cancellationToken).ConfigureAwait(false);
        }

        internal async Task<HttpResponseMessage> SendRangeRequestAsync(
            string url,
            long start,
            long end,
            OperationPauseToken pauseToken,
            CancellationToken cancellationToken)
        {
            if (start < 0) throw new ArgumentOutOfRangeException(nameof(start));
            if (end < start) throw new ArgumentOutOfRangeException(nameof(end));
            return await SendPackageRequestAsync(url, start, end, pauseToken, cancellationToken).ConfigureAwait(false);
        }

        private async Task<HttpResponseMessage> SendPackageRequestAsync(
            string url,
            long? rangeStart,
            long? rangeEnd,
            OperationPauseToken pauseToken,
            CancellationToken cancellationToken)
        {
            Uri current;
            if (!Uri.TryCreate(url, UriKind.Absolute, out current) ||
                !MicrosoftStoreProtocolClient.IsMicrosoftDeliveryUri(current))
            {
                throw new InvalidDataException("程序包下载地址不是受信任的微软分发地址。");
            }

            for (int redirectCount = 0; redirectCount <= HttpRetryPolicy.MaximumRedirects; redirectCount++)
            {
                HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Get, current);
                request.Headers.ConnectionClose = true;
                if (rangeStart.HasValue)
                {
                    request.Headers.Range = new RangeHeaderValue(rangeStart, rangeEnd);
                }
                HttpResponseMessage response;
                try
                {
                    response = await SendDownloadHeadersAsync(
                        request,
                        pauseToken,
                        cancellationToken).ConfigureAwait(false);
                }
                finally
                {
                    request.Dispose();
                }

                Uri actual = response.RequestMessage == null ? null : response.RequestMessage.RequestUri;
                if (actual == null ||
                    !string.Equals(actual.AbsoluteUri, current.AbsoluteUri, StringComparison.OrdinalIgnoreCase))
                {
                    response.Dispose();
                    throw new InvalidDataException("HTTP 处理器绕过了程序的下载重定向验证。");
                }
                if (!HttpRetryPolicy.IsRedirectStatus(response.StatusCode))
                {
                    return response;
                }
                if (redirectCount == HttpRetryPolicy.MaximumRedirects)
                {
                    response.Dispose();
                    throw new InvalidDataException("微软 CDN 下载地址重定向次数过多。");
                }

                Uri location = response.Headers.Location;
                Uri next = location == null
                    ? null
                    : (location.IsAbsoluteUri ? location : new Uri(current, location));
                response.Dispose();
                if (!MicrosoftStoreProtocolClient.IsMicrosoftDeliveryUri(next))
                {
                    throw new InvalidDataException("微软 CDN 将程序包下载重定向到了不受信任的地址。");
                }
                current = next;
            }
            throw new InvalidOperationException("程序包下载重定向状态异常。");
        }

        private async Task<HttpResponseMessage> SendDownloadHeadersAsync(
            HttpRequestMessage request,
            OperationPauseToken pauseToken,
            CancellationToken cancellationToken)
        {
            if (pauseToken == null) pauseToken = new OperationPauseToken(null);
            CancellationToken pauseCancellation = pauseToken.InterruptionToken;
            CancellationToken retryCancellation = pauseToken.RetryInterruptionToken;
            CancellationToken networkCancellation = networkMonitor.InterruptionToken;
            using (CancellationTokenSource requestCancellation =
                CancellationTokenSource.CreateLinkedTokenSource(
                    cancellationToken,
                    pauseCancellation,
                    retryCancellation,
                    networkCancellation))
            using (CancellationTokenSource watchdogCancellation =
                CancellationTokenSource.CreateLinkedTokenSource(
                    cancellationToken,
                    pauseCancellation,
                    retryCancellation,
                    networkCancellation))
            {
                Task<HttpResponseMessage> sendTask = null;
                try
                {
                    sendTask = httpClient.SendAsync(
                        request,
                        HttpCompletionOption.ResponseHeadersRead,
                        requestCancellation.Token);
                    Task timeoutTask = Task.Delay(downloadInactivityTimeout, watchdogCancellation.Token);
                    Task completed = await Task.WhenAny(sendTask, timeoutTask).ConfigureAwait(false);
                    if (completed == sendTask)
                    {
                        watchdogCancellation.Cancel();
                        return await sendTask.ConfigureAwait(false);
                    }
                    requestCancellation.Cancel();
                    request.Dispose();
                    ObserveAbandonedResponse(sendTask);
                    cancellationToken.ThrowIfCancellationRequested();
                    if (pauseCancellation.IsCancellationRequested)
                    {
                        throw new DownloadPausedException();
                    }
                    if (retryCancellation.IsCancellationRequested)
                    {
                        throw new DownloadRetryRequestedException();
                    }
                    if (networkCancellation.IsCancellationRequested)
                    {
                        throw new DownloadNetworkChangedException();
                    }
                    throw new DownloadTransportException(
                        "等待微软 CDN 响应超过 " + FormatDuration(downloadInactivityTimeout) + "，下载连接已停滞。");
                }
                catch (OperationCanceledException exception)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    if (pauseCancellation.IsCancellationRequested)
                    {
                        throw new DownloadPausedException();
                    }
                    if (retryCancellation.IsCancellationRequested)
                    {
                        throw new DownloadRetryRequestedException();
                    }
                    if (networkCancellation.IsCancellationRequested)
                    {
                        throw new DownloadNetworkChangedException();
                    }
                    throw new DownloadTransportException(
                        "等待微软 CDN 响应超过 " + FormatDuration(downloadInactivityTimeout) + "，下载连接已停滞。",
                        exception);
                }
            }
        }

        internal async Task<int> ReadDownloadChunkAsync(
            Stream input,
            byte[] buffer,
            OperationPauseToken pauseToken,
            CancellationToken cancellationToken)
        {
            await pauseToken.WaitWhilePausedAsync(cancellationToken).ConfigureAwait(false);
            CancellationToken pauseCancellation = pauseToken.InterruptionToken;
            CancellationToken retryCancellation = pauseToken.RetryInterruptionToken;
            CancellationToken networkCancellation = networkMonitor.InterruptionToken;
            using (CancellationTokenSource inactivityCancellation =
                CancellationTokenSource.CreateLinkedTokenSource(
                    cancellationToken,
                    pauseCancellation,
                    retryCancellation,
                    networkCancellation))
            {
                Task<int> readTask = null;
                try
                {
                    readTask = input.ReadAsync(
                        buffer,
                        0,
                        buffer.Length,
                        inactivityCancellation.Token);
                    Task timeoutTask = Task.Delay(downloadInactivityTimeout, inactivityCancellation.Token);
                    Task completed = await Task.WhenAny(readTask, timeoutTask).ConfigureAwait(false);
                    if (completed == readTask)
                    {
                        inactivityCancellation.Cancel();
                        return await readTask.ConfigureAwait(false);
                    }
                    inactivityCancellation.Cancel();
                    input.Dispose();
                    ObserveAbandonedRead(readTask);
                    cancellationToken.ThrowIfCancellationRequested();
                    if (pauseCancellation.IsCancellationRequested)
                    {
                        throw new DownloadPausedException();
                    }
                    if (retryCancellation.IsCancellationRequested)
                    {
                        throw new DownloadRetryRequestedException();
                    }
                    if (networkCancellation.IsCancellationRequested)
                    {
                        throw new DownloadNetworkChangedException();
                    }
                    throw new DownloadTransportException(
                        "微软 CDN 连续 " + FormatDuration(downloadInactivityTimeout) + "未返回数据，下载连接已停滞。");
                }
                catch (OperationCanceledException exception)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    if (pauseCancellation.IsCancellationRequested)
                    {
                        input.Dispose();
                        throw new DownloadPausedException();
                    }
                    if (retryCancellation.IsCancellationRequested)
                    {
                        input.Dispose();
                        throw new DownloadRetryRequestedException();
                    }
                    if (networkCancellation.IsCancellationRequested)
                    {
                        input.Dispose();
                        throw new DownloadNetworkChangedException();
                    }
                    throw new DownloadTransportException(
                        "微软 CDN 连续 " + FormatDuration(downloadInactivityTimeout) + "未返回数据，下载连接已停滞。",
                        exception);
                }
                catch (IOException exception) when (!(exception is DownloadTransportException))
                {
                    throw new DownloadTransportException("读取微软 CDN 响应时连接中断。", exception);
                }
            }
        }

        internal async Task WaitForDownloadRetryAsync(
            TimeSpan delay,
            OperationPauseToken pauseToken,
            CancellationToken cancellationToken)
        {
            if (pauseToken == null) pauseToken = new OperationPauseToken(null);
            DateTime deadline = DateTime.UtcNow + delay;
            bool observedUnavailable = false;
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await pauseToken.WaitWhilePausedAsync(cancellationToken).ConfigureAwait(false);
                bool internetAvailable = networkMonitor.HasInternetAccess;
                if (!internetAvailable) observedUnavailable = true;
                if (internetAvailable && observedUnavailable) return;
                TimeSpan remaining = deadline - DateTime.UtcNow;
                if (remaining <= TimeSpan.Zero) return;
                TimeSpan wait = internetAvailable
                    ? remaining
                    : TimeSpan.FromMilliseconds(Math.Min(
                        TimeSpan.FromSeconds(2).TotalMilliseconds,
                        remaining.TotalMilliseconds));

                CancellationToken pauseCancellation = pauseToken.InterruptionToken;
                CancellationToken retryCancellation = pauseToken.RetryInterruptionToken;
                CancellationToken networkCancellation = networkMonitor.InterruptionToken;
                using (CancellationTokenSource interruption =
                    CancellationTokenSource.CreateLinkedTokenSource(
                        cancellationToken,
                        pauseCancellation,
                        retryCancellation,
                        networkCancellation))
                {
                    try
                    {
                        await Task.Delay(wait, interruption.Token).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        if (pauseCancellation.IsCancellationRequested)
                        {
                            await pauseToken.WaitWhilePausedAsync(cancellationToken).ConfigureAwait(false);
                            return;
                        }
                        if (retryCancellation.IsCancellationRequested) return;
                        if (networkCancellation.IsCancellationRequested)
                        {
                            if (networkMonitor.HasInternetAccess) return;
                            continue;
                        }
                        throw;
                    }
                }
                if (internetAvailable || DateTime.UtcNow >= deadline) return;
            }
        }

        internal bool HasInternetAccess { get { return networkMonitor.HasInternetAccess; } }

        private static void ObserveAbandonedResponse(Task<HttpResponseMessage> task)
        {
            task.ContinueWith(
                completed =>
                {
                    if (completed.Status == TaskStatus.RanToCompletion && completed.Result != null)
                    {
                        completed.Result.Dispose();
                    }
                    else if (completed.IsFaulted)
                    {
                        Exception ignored = completed.Exception;
                    }
                },
                CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
        }

        private static void ObserveAbandonedRead(Task<int> task)
        {
            task.ContinueWith(
                completed =>
                {
                    if (completed.IsFaulted)
                    {
                        Exception ignored = completed.Exception;
                    }
                },
                CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
        }

        internal static bool IsRetryableDownloadException(
            Exception exception,
            CancellationToken cancellationToken)
        {
            return exception is TransientHttpRequestException ||
                exception is DownloadTransportException ||
                HttpRetryPolicy.IsTransientTransportException(exception, cancellationToken);
        }

        internal TimeSpan DownloadRecoveryWindow { get { return downloadRecoveryWindow; } }

        internal void LogMessage(string message)
        {
            log(message);
        }

        private static string GetExceptionSummary(Exception exception)
        {
            Exception current = exception;
            while (current != null && current.InnerException != null)
            {
                current = current.InnerException;
            }
            return current == null || string.IsNullOrWhiteSpace(current.Message) ? "未知网络错误" : current.Message;
        }

        private static string FormatDuration(TimeSpan duration)
        {
            if (duration.TotalHours >= 1)
            {
                return string.Format(CultureInfo.InvariantCulture, "{0} 小时 {1} 分", (int)duration.TotalHours, duration.Minutes);
            }
            if (duration.TotalMinutes >= 1)
            {
                return string.Format(CultureInfo.InvariantCulture, "{0} 分 {1} 秒", (int)duration.TotalMinutes, duration.Seconds);
            }
            return Math.Max(1, (int)Math.Ceiling(duration.TotalSeconds)).ToString(CultureInfo.InvariantCulture) + " 秒";
        }

        private static PackageProfile ValidateStagedPackage(
            string stagingRoot,
            PackageMetadata package,
            PackageProfile profile)
        {
            string manifestPath = Path.Combine(stagingRoot, "AppxManifest.xml");
            if (!File.Exists(manifestPath) || new FileInfo(manifestPath).Length == 0 || profile == null)
            {
                throw new InvalidDataException("解包结果中没有 AppxManifest.xml。");
            }

            if (!string.Equals(profile.PackageName, package.packageName, StringComparison.Ordinal) ||
                !string.Equals(profile.Version, package.version, StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidDataException("解包程序包的身份或版本与微软元数据不一致。");
            }

            string executable = PackageProfileReader.GetExecutablePath(stagingRoot, profile);
            string executableDirectory = Path.GetDirectoryName(executable);
            if (string.IsNullOrWhiteSpace(executableDirectory))
            {
                throw new InvalidDataException("无法确定清单声明的主程序目录。");
            }
            string[] requiredFiles =
            {
                Path.Combine(executableDirectory, "resources", "app.asar"),
                Path.Combine(executableDirectory, "resources", "codex.exe")
            };
            foreach (string fullPath in requiredFiles)
            {
                if (!File.Exists(fullPath) || new FileInfo(fullPath).Length == 0)
                {
                    throw new InvalidDataException("解包结果缺少关键运行组件：" + fullPath);
                }
            }
            if (!File.Exists(executable) || new FileInfo(executable).Length == 0)
            {
                throw new InvalidDataException("清单声明的主程序不存在：" + profile.ExecutableRelativePath);
            }
            return profile;
        }

        private static string ComputeSha256Base64(string path)
        {
            using (FileStream stream = File.OpenRead(path))
            using (SHA256 sha = SHA256.Create())
            {
                return Convert.ToBase64String(sha.ComputeHash(stream));
            }
        }

        private void TryDeleteFile(string path, string description)
        {
            if (!File.Exists(path))
            {
                return;
            }
            try
            {
                NativeFileSystem.DeleteFile(path);
            }
            catch (Exception exception)
            {
                log("警告：无法清理" + description + "：" + path + "。" + exception.Message);
            }
        }

        internal class DownloadTransportException : IOException
        {
            internal DownloadTransportException(string message, Exception innerException = null)
                : base(message, innerException)
            {
            }
        }

        internal sealed class DownloadPausedException : OperationCanceledException
        {
        }

        internal sealed class DownloadRetryRequestedException : OperationCanceledException
        {
        }

        internal sealed class DownloadNetworkChangedException : DownloadTransportException
        {
            internal DownloadNetworkChangedException()
                : base("检测到系统网络状态或地址发生变化，正在重新确认微软 CDN 连接。")
            {
            }
        }

        public void Dispose()
        {
            httpClient.Dispose();
            networkMonitor.Dispose();
        }
    }
}
