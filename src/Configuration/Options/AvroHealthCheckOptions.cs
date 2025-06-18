namespace KsqlDsl.Configuration.Options
{
    public class AvroHealthCheckOptions
    {
        /// <summary>
        /// 警告レベルのキャッシュヒット率閾値（デフォルト: 70%）
        /// </summary>
        public double WarningHitRateThreshold { get; set; } = 0.70;

        /// <summary>
        /// 危険レベルのキャッシュヒット率閾値（デフォルト: 50%）
        /// </summary>
        public double CriticalHitRateThreshold { get; set; } = 0.50;

        /// <summary>
        /// 警告レベルのスロー操作率閾値（デフォルト: 5%）
        /// </summary>
        public double WarningSlowOperationRateThreshold { get; set; } = 0.05;

        /// <summary>
        /// 危険レベルのスロー操作率閾値（デフォルト: 15%）
        /// </summary>
        public double CriticalSlowOperationRateThreshold { get; set; } = 0.15;

        /// <summary>
        /// 警告レベルのキャッシュサイズ閾値（デフォルト: 5000）
        /// </summary>
        public int WarningCacheSizeThreshold { get; set; } = 5000;

        /// <summary>
        /// 危険レベルのキャッシュサイズ閾値（デフォルト: 10000）
        /// </summary>
        public int CriticalCacheSizeThreshold { get; set; } = 10000;

        /// <summary>
        /// 危険レベルの平均操作時間閾値（ミリ秒）（デフォルト: 200ms）
        /// </summary>
        public double CriticalAverageOperationTimeMs { get; set; } = 200.0;

        /// <summary>
        /// 最小成功率閾値（デフォルト: 95%）
        /// </summary>
        public double MinimumSuccessRate { get; set; } = 0.95;

        /// <summary>
        /// 評価に必要な最小リクエスト数（デフォルト: 100）
        /// </summary>
        public long MinimumRequestsForEvaluation { get; set; } = 100;

        /// <summary>
        /// 評価に必要な最小操作数（デフォルト: 50）
        /// </summary>
        public long MinimumOperationsForEvaluation { get; set; } = 50;

        /// <summary>
        /// 最近のスロー操作の最大許容数（10分間）（デフォルト: 20）
        /// </summary>
        public int MaxRecentSlowOperations { get; set; } = 20;

        /// <summary>
        /// 危険認定前の最大スローエンティティ数（デフォルト: 5）
        /// </summary>
        public int MaxSlowEntitiesBeforeCritical { get; set; } = 5;

        /// <summary>
        /// 警告レベルの最近の失敗率閾値（30分間）（デフォルト: 2%）
        /// </summary>
        public double WarningRecentFailureRateThreshold { get; set; } = 0.02;

        /// <summary>
        /// 危険レベルの最近の失敗率閾値（30分間）（デフォルト: 10%）
        /// </summary>
        public double CriticalRecentFailureRateThreshold { get; set; } = 0.10;

        /// <summary>
        /// 最大キャッシュ効率比率（デフォルト: 2.0）
        /// 設計理由：リクエスト数に対するキャッシュ項目数の比率上限
        /// </summary>
        public double MaxCacheEfficiencyRatio { get; set; } = 2.0;

        /// <summary>
        /// 最大キャッシュ成長率（1時間あたり）（デフォルト: 1000）
        /// </summary>
        public double MaxCacheGrowthRatePerHour { get; set; } = 1000.0;
    }

}
